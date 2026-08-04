package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/QuantaStream/quantastream/qsfixture"
)

func main() {
	baselinePath := flag.String("baseline", "", "baseline native ingest benchmark JSON report")
	targetPath := flag.String("target", "", "target native ingest benchmark JSON report")
	outPath := flag.String("out", "", "optional markdown output path")
	flag.Parse()

	if *baselinePath == "" || *targetPath == "" {
		fmt.Fprintln(os.Stderr, "usage: ingest-benchmark-compare -baseline baseline.json -target target.json [-out comparison.md]")
		os.Exit(2)
	}

	baseline, err := qsfixture.ReadNativeIngestBenchmarkReport(*baselinePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read baseline report: %v\n", err)
		os.Exit(1)
	}
	target, err := qsfixture.ReadNativeIngestBenchmarkReport(*targetPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read target report: %v\n", err)
		os.Exit(1)
	}

	comparison := qsfixture.CompareNativeIngestBenchmarkReports(baseline, target)
	if *outPath != "" {
		if err := qsfixture.WriteNativeIngestBenchmarkComparisonMarkdown(*outPath, comparison); err != nil {
			fmt.Fprintf(os.Stderr, "write comparison: %v\n", err)
			os.Exit(1)
		}
		return
	}
	fmt.Print(qsfixture.RenderNativeIngestBenchmarkComparisonMarkdown(comparison))
}
