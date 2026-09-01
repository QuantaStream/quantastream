package admin

import (
	"fmt"

	"github.com/QuantaStream/quantastream/core"
)

// WALCmd groups local write-ahead log inspection operations.
type WALCmd struct {
	Validate WALValidateCmd `cmd:"" help:"Validate a local write-ahead log and checkpoint pair."`
	Plan     WALPlanCmd     `cmd:"" help:"Print the local write-ahead log recovery plan."`
}

type WALValidateCmd struct {
	Path string `help:"Local WAL path. Supports file:///path or a local path." required:""`
}

type WALPlanCmd struct {
	Path string `help:"Local WAL path. Supports file:///path or a local path." required:""`
}

func (c *WALValidateCmd) Run(ctx *Context) error {
	summary, err := core.ValidateLocalWAL(c.Path)
	if err != nil {
		return err
	}
	fmt.Printf("wal_valid=%s\n", c.Path)
	printWALSummary(summary)
	return nil
}

func (c *WALPlanCmd) Run(ctx *Context) error {
	plan, err := core.PlanLocalWALRecovery(c.Path)
	if err != nil {
		return err
	}
	fmt.Printf("wal_plan=%s\n", c.Path)
	printWALRecoveryPlan(plan)
	return nil
}

func printWALSummary(summary core.LocalWALSummary) {
	fmt.Printf("wal_records=%d\n", summary.RecordCount)
	fmt.Printf("wal_last_lsn=%d\n", summary.LastLSN)
	fmt.Printf("wal_bytes=%d\n", summary.ByteCount)
	fmt.Printf("wal_torn_tail_bytes=%d\n", summary.TornTailBytes)
	if summary.TornTailLine != 0 {
		fmt.Printf("wal_torn_tail_line=%d\n", summary.TornTailLine)
	}
	fmt.Printf("wal_checkpoint_exists=%t\n", summary.CheckpointExists)
	fmt.Printf("wal_checkpoint_lsn=%d\n", summary.CheckpointLSN)
	if summary.CheckpointPath != "" {
		fmt.Printf("wal_checkpoint_path=%s\n", summary.CheckpointPath)
	}
}

func printWALRecoveryPlan(plan core.LocalWALRecoveryPlan) {
	fmt.Printf("wal_path=%s\n", plan.WALPath)
	fmt.Printf("wal_checkpoint_path=%s\n", plan.CheckpointPath)
	fmt.Printf("wal_checkpoint_exists=%t\n", plan.CheckpointExists)
	fmt.Printf("wal_checkpoint_lsn=%d\n", plan.CheckpointLSN)
	fmt.Printf("wal_last_lsn=%d\n", plan.LastLSN)
	fmt.Printf("wal_records=%d\n", plan.RecordCount)
	fmt.Printf("wal_torn_tail_bytes=%d\n", plan.TornTailBytes)
	if plan.TornTailLine != 0 {
		fmt.Printf("wal_torn_tail_line=%d\n", plan.TornTailLine)
	}
	fmt.Printf("wal_checkpointed_records=%d\n", plan.CheckpointedRecordCount)
	fmt.Printf("wal_replay_records=%d\n", plan.ReplayRecordCount())
	fmt.Printf("wal_pending_records=%d\n", plan.PendingRecordCount())
	fmt.Printf("wal_replay_commit_boundaries=%d\n", plan.ReplayCommitBoundaryCount)
	fmt.Printf("wal_needs_replay=%t\n", plan.NeedsReplay())
	fmt.Printf("wal_has_pending_tail=%t\n", plan.HasPendingTail())
}
