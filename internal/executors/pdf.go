package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"task-scheduler/internal"
)

// ============================================================
// PDF Executor — Simulates PDF document generation
// ============================================================

// PDFExecutor handles PDF generation tasks (receipts, invoices, reports).
type PDFExecutor struct {
	logger *slog.Logger
}

// NewPDFExecutor creates a new PDFExecutor.
func NewPDFExecutor() *PDFExecutor {
	return &PDFExecutor{
		logger: slog.Default().With("executor", "pdf"),
	}
}

// Type returns the executor type identifier.
func (p *PDFExecutor) Type() string {
	return "pdf"
}

// pdfConfig is the expected configuration for PDF generation tasks.
type pdfConfig struct {
	Template string `json:"template"` // "receipt", "invoice", "report"
	Title    string `json:"title"`
	Format   string `json:"format"` // "A4", "Letter"
}

// Execute simulates generating a PDF document.
func (p *PDFExecutor) Execute(ctx context.Context, msg internal.TaskMessage) (*internal.ExecutionResult, error) {
	var cfg pdfConfig
	if err := json.Unmarshal(msg.Config, &cfg); err != nil {
		return nil, fmt.Errorf("invalid PDF config: %w", err)
	}

	if cfg.Format == "" {
		cfg.Format = "A4"
	}

	p.logger.Info("generating PDF",
		"template", cfg.Template,
		"title", cfg.Title,
		"format", cfg.Format,
		"step_id", msg.StepID,
	)

	// PDF generation is typically slower (500ms-2s)
	processingTime := time.Duration(500+rand.Intn(1500)) * time.Millisecond
	time.Sleep(processingTime)

	// Simulate ~5% failure rate (lower than network-dependent tasks)
	if rand.Float64() < 0.05 {
		return nil, fmt.Errorf("PDF rendering failed: template '%s' contains invalid markup", cfg.Template)
	}

	// Simulate generated PDF metadata
	pageCount := rand.Intn(5) + 1
	fileSize := (rand.Intn(500) + 100) * 1024 // 100KB - 600KB
	fileName := fmt.Sprintf("%s_%d.pdf", cfg.Template, time.Now().Unix())
	filePath := fmt.Sprintf("/generated/pdfs/%s", fileName)

	p.logger.Info("PDF generated successfully",
		"template", cfg.Template,
		"file_name", fileName,
		"file_path", filePath,
		"pages", pageCount,
		"size_bytes", fileSize,
		"duration_ms", processingTime.Milliseconds(),
	)

	output, _ := json.Marshal(map[string]interface{}{
		"file_name":   fileName,
		"file_path":   filePath,
		"template":    cfg.Template,
		"pages":       pageCount,
		"size_bytes":  fileSize,
		"format":      cfg.Format,
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	})

	return &internal.ExecutionResult{
		Output:   output,
		Duration: processingTime,
	}, nil
}
