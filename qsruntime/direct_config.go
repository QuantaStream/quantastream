package qsruntime

import "github.com/QuantaStream/quantastream/qsbridge"

// DefaultDirectServicePort is the current default port used by local Quanta runtime paths.
const DefaultDirectServicePort = 4000

// DirectRuntimeConfig captures the inputs needed to build a direct in-process runtime later.
type DirectRuntimeConfig struct {
	BaseDir         string
	ConsulAddress   string
	ServicePort     int
	SessionPoolSize int
}

// DirectQuantaSourceArgs names the future source.NewQuantaSource constructor arguments.
type DirectQuantaSourceArgs struct {
	BaseDir         string
	ConsulAddress   string
	ServicePort     int
	SessionPoolSize int
}

// NewDirectRuntimeConfig returns a direct runtime configuration.
func NewDirectRuntimeConfig(baseDir, consulAddress string, servicePort, sessionPoolSize int) DirectRuntimeConfig {
	return DirectRuntimeConfig{
		BaseDir:         baseDir,
		ConsulAddress:   consulAddress,
		ServicePort:     servicePort,
		SessionPoolSize: sessionPoolSize,
	}
}

// WithDefaults returns a copy with qsruntime-level defaults applied.
func (c DirectRuntimeConfig) WithDefaults() DirectRuntimeConfig {
	if c.ServicePort == 0 {
		c.ServicePort = DefaultDirectServicePort
	}
	return c
}

// Validate returns diagnostics for invalid direct runtime configuration.
func (c DirectRuntimeConfig) Validate() qsbridge.DiagnosticSet {
	var diagnostics qsbridge.DiagnosticSet
	if c.ServicePort < 0 {
		diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInvalidExecutionOption,
			qsbridge.PhaseExecute,
			"direct runtime service port cannot be negative",
		))
	}
	if c.SessionPoolSize < 0 {
		diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInvalidExecutionOption,
			qsbridge.PhaseExecute,
			"direct runtime session pool size cannot be negative",
		))
	}
	return diagnostics
}

// QuantaSourceArgs returns constructor arguments for the future legacy source adapter.
func (c DirectRuntimeConfig) QuantaSourceArgs() DirectQuantaSourceArgs {
	withDefaults := c.WithDefaults()
	return DirectQuantaSourceArgs{
		BaseDir:         withDefaults.BaseDir,
		ConsulAddress:   withDefaults.ConsulAddress,
		ServicePort:     withDefaults.ServicePort,
		SessionPoolSize: withDefaults.SessionPoolSize,
	}
}
