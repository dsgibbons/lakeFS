package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bufbuild/connect-go"
	"github.com/treeverse/lakefs/gen/proto/go/lakefs/v1"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/upload"
)

type Service struct {
	Catalog      catalog.Interface
	BlockAdapter block.Adapter
	PathProvider upload.PathProvider
}

func (s *Service) ListObjects(ctx context.Context, req *connect.Request[lakefsv1.ListObjectsRequest]) (*connect.Response[lakefsv1.ListObjectsResponse], error) {
	repo, err := s.Catalog.GetRepository(ctx, req.Msg.Repository)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnknown, err)
	}

	limit := int(req.Msg.PageSize)
	if limit == 0 {
		limit = DefaultMaxPerPage
	}
	res, hasMore, err := s.Catalog.ListEntries(ctx,
		req.Msg.Repository,
		req.Msg.Ref,
		req.Msg.Prefix,
		req.Msg.PageToken,
		req.Msg.Delimiter,
		limit,
	)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnknown, err)
	}

	var objs []*lakefsv1.ObjectInfo
	for _, entry := range res {
		qk, err := block.ResolveNamespace(repo.StorageNamespace, entry.PhysicalAddress, entry.AddressType.ToIdentifierType())
		if err != nil {
			return nil, connect.NewError(connect.CodeUnknown, err)
		}

		var obj *lakefsv1.ObjectInfo
		if entry.CommonLevel {
			obj = &lakefsv1.ObjectInfo{
				Path:     entry.Path,
				PathType: lakefsv1.PathType_PATH_TYPE_COMMON_PREFIX,
			}
		} else {
			obj = &lakefsv1.ObjectInfo{
				Checksum:        entry.Checksum,
				Mtime:           timestamppb.New(entry.CreationDate),
				Path:            entry.Path,
				PhysicalAddress: qk.Format(),
				PathType:        lakefsv1.PathType_PATH_TYPE_OBJECT,
				SizeBytes:       entry.Size,
				ContentType:     entry.ContentType,
			}
			if req.Msg.UserMetadata {
				obj.UserMetadata = entry.Metadata
			}
		}
		objs = append(objs, obj)
	}

	response := &lakefsv1.ListObjectsResponse{
		Objects: objs,
	}
	if hasMore && len(objs) > 0 {
		response.NextPageToken = objs[len(objs)-1].Path
	}
	return connect.NewResponse(response), nil
}

func (s *Service) GetObject(ctx context.Context, req *connect.Request[lakefsv1.GetObjectRequest], server *connect.ServerStream[lakefsv1.GetObjectResponse]) error {
	repo, err := s.Catalog.GetRepository(ctx, req.Msg.Repository)
	if err != nil {
		return err
	}

	// read the FS entry
	entry, err := s.Catalog.GetEntry(ctx, req.Msg.Repository, req.Msg.Ref, req.Msg.Path, catalog.GetEntryParams{})
	if err != nil {
		return err
	}
	if entry.Expired {
		return connect.NewError(connect.CodeUnknown, catalog.ErrExpired)
	}

	// if pre-sign, return a redirect
	pointer := block.ObjectPointer{StorageNamespace: repo.StorageNamespace, Identifier: entry.PhysicalAddress}
	if req.Msg.PreSigned {
		location, err := s.BlockAdapter.GetPreSignedURL(ctx, pointer, block.PreSignModeRead)
		if err != nil {
			return err
		}
		return server.Send(&lakefsv1.GetObjectResponse{PreSignedUrl: location})
	}

	// setup response
	var reader io.ReadCloser

	// handle partial response if byte range supplied
	if req.Msg.Range != "" {
		rng, err := httputil.ParseRange(req.Msg.Range, entry.Size)
		if err != nil {
			return connect.NewError(connect.CodeUnknown, err)
		}
		reader, err = s.BlockAdapter.GetRange(ctx, pointer, rng.StartOffset, rng.EndOffset)
		if err != nil {
			return connect.NewError(connect.CodeUnknown, err)
		}
		defer func() {
			_ = reader.Close()
		}()

		server.ResponseHeader().Add("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.StartOffset, rng.EndOffset, entry.Size))
		server.ResponseHeader().Add("Content-Length", fmt.Sprintf("%d", rng.EndOffset-rng.StartOffset+1))
	} else {
		reader, err = s.BlockAdapter.Get(ctx, pointer, entry.Size)
		if err != nil {
			return connect.NewError(connect.CodeUnknown, err)
		}
		defer func() {
			_ = reader.Close()
		}()
		server.ResponseHeader().Add("Content-Length", fmt.Sprint(entry.Size))
	}

	etag := httputil.ETag(entry.Checksum)
	lastModified := httputil.HeaderTimestamp(entry.CreationDate)
	server.ResponseHeader().Add("ETag", etag)
	server.ResponseHeader().Add("Last-Modified", lastModified)
	server.ResponseHeader().Add("Content-Type", entry.ContentType)

	buf := make([]byte, 1024*8)
	for {
		num, err := reader.Read(buf)
		if num > 0 {
			if err := server.Send(&lakefsv1.GetObjectResponse{Chunk: buf[:num]}); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) UploadObject(ctx context.Context, req *connect.ClientStream[lakefsv1.UploadObjectRequest]) (*connect.Response[lakefsv1.UploadObjectResponse], error) {
	if !req.Receive() {
		return nil, connect.NewError(connect.CodeUnknown, io.EOF)
	}
	// upload info
	info := req.Msg().GetInfo()
	if info == nil {
		return nil, connect.NewError(connect.CodeUnknown, io.EOF)
	}

	repo, err := s.Catalog.GetRepository(ctx, info.Repository)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnknown, err)
	}

	// check if branch exists - it is still a possibility, but we don't want to upload large object when the branch was not there in the first place
	branchExists, err := s.Catalog.BranchExists(ctx, info.Repository, info.Branch)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnknown, err)
	}
	if !branchExists {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("branch '%s' not found", info.Branch))
	}

	// read chunks
	reader := &UploadChunkReader{
		Req: req,
	}
	address := s.PathProvider.NewPath()
	blob, err := upload.WriteBlob(ctx, s.BlockAdapter, repo.StorageNamespace, address, reader, -1, block.PutOpts{})
	if err != nil {
		return nil, connect.NewError(connect.CodeUnknown, err)
	}

	// write metadata
	writeTime := time.Now()
	entryBuilder := catalog.NewDBEntryBuilder().
		Path(info.Path).
		PhysicalAddress(blob.PhysicalAddress).
		CreationDate(writeTime).
		Size(blob.Size).
		Checksum(blob.Checksum).
		ContentType(info.ContentType).
		Metadata(info.Metadata)
	if blob.RelativePath {
		entryBuilder.AddressType(catalog.AddressTypeRelative)
	} else {
		entryBuilder.AddressType(catalog.AddressTypeFull)
	}
	entry := entryBuilder.Build()

	err = s.Catalog.CreateEntry(ctx, info.Repository, info.Branch, entry, graveler.WithIfAbsent(!info.AllowOverwrite))
	if errors.Is(err, graveler.ErrPreconditionFailed) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}

	resp := connect.NewResponse(&lakefsv1.UploadObjectResponse{
		Path:            entry.Path,
		PhysicalAddress: entry.PhysicalAddress,
		Checksum:        entry.Checksum,
		SizeBytes:       entry.Size,
		Mtime:           timestamppb.New(entry.CreationDate),
		Metadata:        entry.Metadata,
		ContentType:     entry.ContentType,
	})
	return resp, nil
}

type UploadChunkReader struct {
	Req   *connect.ClientStream[lakefsv1.UploadObjectRequest]
	chunk []byte
}

func (u *UploadChunkReader) Read(p []byte) (n int, err error) {
	written := 0
	for written < len(p) {
		if len(u.chunk) == 0 {
			if !u.Req.Receive() {
				return written, io.EOF
			}
			if err := u.Req.Err(); err != nil {
				return 0, err
			}
			u.chunk = u.Req.Msg().GetChunk()
			if u.chunk == nil {
				return written, io.EOF
			}
		}
		if len(u.chunk) > 0 {
			w := copy(p[written:], u.chunk)
			written += w
			u.chunk = u.chunk[w:]
		}
	}
	return written, nil
}
