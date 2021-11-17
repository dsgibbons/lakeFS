package stats

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/cloud/aws"
	"github.com/treeverse/lakefs/pkg/cloud/azure"
	"github.com/treeverse/lakefs/pkg/cloud/gcp"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

const BlockstoreTypeKey = "blockstore_type"

type MetadataEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Metadata struct {
	InstallationID string          `json:"installation_id"`
	Entries        []MetadataEntry `json:"entries"`
}

func NewMetadata(ctx context.Context, logger logging.Logger, blockstoreType string, authMetadataManager auth.MetadataManager, cloudMetadataProvider cloud.MetadataProvider) *Metadata {
	res := &Metadata{}
	authMetadata, err := authMetadataManager.Write(ctx)
	if err != nil {
		logger.WithError(err).Debug("failed to collect account metadata")
	}
	for k, v := range authMetadata {
		if k == auth.InstallationIDKeyName {
			res.InstallationID = v
		}
		res.Entries = append(res.Entries, MetadataEntry{Name: k, Value: v})
	}
	if cloudMetadataProvider != nil {
		cloudMetadata := cloudMetadataProvider.GetMetadata(ctx)
		for k, v := range cloudMetadata {
			res.Entries = append(res.Entries, MetadataEntry{Name: k, Value: v})
		}
	}
	res.Entries = append(res.Entries, MetadataEntry{Name: BlockstoreTypeKey, Value: blockstoreType})
	return res
}

func BuildMetadataProvider(logger logging.Logger, c *config.Config) (cloud.MetadataProvider, error) {
	switch c.GetBlockstoreType() {
	case block.BlockstoreTypeGS:
		return gcp.NewMetadataProvider(logger), nil
	case block.BlockstoreTypeS3:
		cfg, err := c.GetAwsConfig()
		if err != nil {
			return nil, fmt.Errorf("get AWS config: %w", err)
		}
		return aws.NewMetadataProvider(logger, cfg.Config), nil
	case block.BlockstoreTypeAzure:
		return azure.NewMetadataProvider(logger), nil
	default:
		return nil, nil
	}
}
