package regression

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

func WriteTable(w io.Writer, report Report) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SESSION\tPARTIAL\tWINDOWS\tLABEL_COV\tEVAL_COV\tPREC\tRECALL\tF1\tCHAT_P95\tASR_P95\tPIPE_P95\tREGRESSIONS"); err != nil {
		return err
	}

	regressionsBySession := map[string]int{}
	if report.Comparison != nil {
		for _, regression := range report.Comparison.Regressions {
			regressionsBySession[regression.SessionID]++
		}
	}

	for _, session := range report.Sessions {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%t\t%d/%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\n",
			session.SessionID,
			session.Metrics.Partial,
			session.Metrics.EvaluatedWindows,
			session.Metrics.TotalWindows,
			formatRatio(session.Metrics.ProofLabelCoverage),
			formatRatio(session.Metrics.EvaluationCoverage),
			formatRatio(session.Metrics.EventPrecision),
			formatRatio(session.Metrics.EventRecall),
			formatRatio(session.Metrics.EventF1),
			formatLatency(session.Metrics.LatencyP95MS, "chat_analysis"),
			formatLatency(session.Metrics.LatencyP95MS, "transcript_asr"),
			formatLatency(session.Metrics.LatencyP95MS, "transcript_pipeline"),
			regressionsBySession[session.SessionID],
		); err != nil {
			return err
		}
	}

	if report.Comparison != nil && len(report.Comparison.Regressions) > 0 {
		if _, err := fmt.Fprintln(tw); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(tw, "REGRESSION\tSESSION\tMETRIC\tBASELINE\tCURRENT\tTHRESHOLD"); err != nil {
			return err
		}
		for _, regression := range report.Comparison.Regressions {
			if _, err := fmt.Fprintf(
				tw,
				"%s\t%s\t%s\t%s\t%s\t%s\n",
				regression.Message,
				regression.SessionID,
				regression.Metric,
				formatNumber(regression.Baseline),
				formatNumber(regression.Current),
				formatNumber(regression.Threshold),
			); err != nil {
				return err
			}
		}
	}
	return tw.Flush()
}

func formatRatio(value *float64) string {
	if value == nil {
		return "-"
	}
	return fmt.Sprintf("%.4f", *value)
}

func formatLatency(values map[string]*float64, key string) string {
	value := values[key]
	if value == nil {
		return "-"
	}
	return fmt.Sprintf("%.0fms", *value)
}

func formatNumber(value *float64) string {
	if value == nil {
		return "-"
	}
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.4f", *value), "0"), ".")
}
