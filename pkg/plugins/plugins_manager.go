package plugins

import (
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/treeverse/lakefs/pkg/logging"
	"os"
	"os/exec"
)

// PluginGroupType specifies the group name of related plugins
// For example, "diff" is a group of plugins that know how to perform a diff.
type PluginGroupType string

// PluginType specifies a type of plugin
// For example, "delta" is a plugin.
type PluginType string

// PluginIdentity identifies the plugin using a Key and Secret, and supplies the Version it expects the plugin to be of.
type PluginIdentity struct {
	Version uint
	Key     string
	Secret  string
}
type PluginTypeConfigMap map[PluginType]plugin.ClientConfig

// PluginGroupTypeMap example:
/*
{
	"diff": {
		"delta": plugin.ClientConfig{
			HandshakeConfig: plugin.HandshakeConfig{
				ProtocolVersion:  1,
				MagicCookieKey:   "key",
				MagicCookieValue: "value",
			},
			Cmd: exec.Command("path/to/executable/diff/delta"),
			AllowedProtocols: []plugin.Protocol{
				plugin.ProtocolGRPC,
			},
			Plugins: map[string]plugin.Plugin{
				"deltaDiff": DeltaGRPCPlugin,
			},
		},
		"iceberg: {
			HandshakeConfig: plugin.HandshakeConfig{
				ProtocolVersion:  1,
				MagicCookieKey:   "key",
				MagicCookieValue: "value",
			},
			Cmd: exec.Command("path/to/executable/diff/iceberg"),
			AllowedProtocols: []plugin.Protocol{
				plugin.ProtocolGRPC,
			},
			Plugins: map[string]plugin.Plugin{
				"icebergDiff": IcebergGRPCPlugin
			},
		}
	},
	"merge": {
		"delta": {
			HandshakeConfig: plugin.HandshakeConfig{
				ProtocolVersion:  1,
				MagicCookieKey:   "key",
				MagicCookieValue: "value",
			},
			Cmd: exec.Command("path/to/executable/merge/delta"),
			AllowedProtocols: []plugin.Protocol{
				plugin.ProtocolGRPC,
			},
			Plugins: map[string]plugin.Plugin{
				"deltaMerge": DeltaGRPCPlugin,
			},
		},
		"iceberg: {
			HandshakeConfig: plugin.HandshakeConfig{
				ProtocolVersion:  1,
				MagicCookieKey:   "key",
				MagicCookieValue: "value",
			},
			Cmd: exec.Command("path/to/executable/merge/iceberg"),
			AllowedProtocols: []plugin.Protocol{
				plugin.ProtocolGRPC,
			},
			Plugins: map[string]plugin.Plugin{
				"icebergMerge": IcebergGRPCPlugin
			},
		}
	},
}
*/
type PluginGroupTypeMap map[PluginGroupType]PluginTypeConfigMap

// The Manager holds the different types of plugins that can be used in the plugin system
type Manager struct {
	pluginTypes PluginGroupTypeMap
}

func (m *Manager) AddPluginType(pluginGroupType PluginGroupType, pluginType PluginType, pluginTypeConfig plugin.ClientConfig) {
	if m.pluginTypes == nil {
		m.pluginTypes = make(PluginGroupTypeMap)
	}
	ptcm, ok := m.pluginTypes[pluginGroupType]
	if !ok {
		ptcm := make(PluginTypeConfigMap)
		m.pluginTypes[pluginGroupType] = ptcm
	}
	ptcm[pluginType] = pluginTypeConfig
}

// WrappedPluginClient generates a ClientWrapper that wraps the go-plugin client.
//
// It accepts two parameters: the top PluginGroupType and the required PluginType under it.
// For example, PluginGroupType = "diff", PluginType = "delta" will generate a ClientWrapper with a client that performs
// diffs over Delta Lake tables.
// It also returns a DestroyClientFunc
func (m *Manager) WrappedPluginClient(pluginGroupType PluginGroupType, pluginType PluginType) (*ClientWrapper, error) {
	ptpp, ok := m.pluginTypes[pluginGroupType]
	if !ok {
		return nil, fmt.Errorf("unknown plugin group %s", pluginGroupType)
	}
	clientConfig, ok := ptpp[pluginType]
	if !ok {
		return nil, fmt.Errorf("unknown plugin type %s under plugin group %s", pluginType, pluginGroupType)
	}
	return newClient(fmt.Sprintf("%s_%s", pluginGroupType, pluginType), clientConfig)
}

// newClient generates a ClientWrapper that wraps the go-plugin client.
func newClient(clientName string, clientConfig plugin.ClientConfig) (*ClientWrapper, error) {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("%s_logger", clientName),
		Output: os.Stdout,
		Level:  hclog.Debug,
	})
	clientConfig.Logger = logger
	client := plugin.NewClient(&clientConfig)
	return &ClientWrapper{
		Client: client,
		Log:    logging.Default(),
	}, nil
}

// ClientConfig generates a plugin.ClientConfig struct to be used to configure the Manager's plugins with the
// corresponding configurations
func ClientConfig(identity PluginIdentity, cmd exec.Cmd, plugins map[string]plugin.Plugin) plugin.ClientConfig {
	handshakeConfig := plugin.HandshakeConfig{
		ProtocolVersion:  identity.Version,
		MagicCookieKey:   identity.Key,
		MagicCookieValue: identity.Secret,
	}
	allowedProtocols := []plugin.Protocol{
		plugin.ProtocolGRPC,
	}
	return plugin.ClientConfig{
		HandshakeConfig:  handshakeConfig,
		Cmd:              &cmd,
		AllowedProtocols: allowedProtocols,
		Plugins:          plugins,
	}
}
