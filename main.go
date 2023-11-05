package caddy_resumable_uploads

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

var (
	InteropVersion = "4"
	IncompleteExt  = ".incomplete"
	// Interface guards
	_ caddy.Provisioner           = (*Middleware)(nil)
	_ caddy.Validator             = (*Middleware)(nil)
	_ caddyhttp.MiddlewareHandler = (*Middleware)(nil)
	_ caddyfile.Unmarshaler       = (*Middleware)(nil)
)

func init() {
	caddy.RegisterModule(Middleware{})
	httpcaddyfile.RegisterHandlerDirective("resumable_uploads", parseCaddyfile)
}

type Middleware struct {
	// Internal logger
	logger *zap.Logger
	// Path to store temporary files. Defaults to os.TempDir()/resumable_uploads
	TmpDir string `json:"output_path,omitempty"`
}

// ==== Caddy Module Interface ====

func (Middleware) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.resumable_uploads",
		New: func() caddy.Module { return new(Middleware) },
	}
}

func (m *Middleware) Provision(ctx caddy.Context) error {
	m.logger = ctx.Logger()
	if m.TmpDir == "" {
		m.TmpDir = filepath.Join(os.TempDir(), "resumable_uploads")
	}
	m.logger.Info("Provisioning", zap.String("tmpdir", m.TmpDir))
	return os.MkdirAll(m.TmpDir, os.ModePerm)
}

func (h *Middleware) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	// for d.Next() {
	// 	for d.NextBlock(0) {
	// 		switch d.Val() {
	// 		case "location":
	// 			if !d.Args(&h.Location) {
	// 				return d.ArgErr()
	// 			}
	// 		}
	// 	}
	// }
	return nil
}

func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var m Middleware
	err := m.UnmarshalCaddyfile(h.Dispenser)
	return m, err
}

func (m *Middleware) Validate() error {
	// TODO
	return nil
}

func (m Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, PATCH, HEAD, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Upload-Draft-Interop-Version, Upload-Offset, Upload-Complete, Location")

	m.logger.Info("ServeHTTP", zap.String("method", r.Method), zap.String("url", getUrl(r)))

	var err error
	switch r.Method {
	case http.MethodOptions:
		w.WriteHeader(http.StatusOK)
		return nil
	case http.MethodPost:
		err = m.UploadCreationHandler(w, r)
	case http.MethodHead:
		err = m.OffsetRetrievingHandler(w, r)
	case http.MethodPatch:
		err = m.UploadAppendingHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	if err != nil {
		m.logger.Error("ServeHTTP", zap.Error(err))
	}
	return err
}

// ==== Resumable Uploads ====

func (m *Middleware) UploadCreationHandler(w http.ResponseWriter, r *http.Request) error {
	var uploadId string
	var file *os.File
	var err error

	// Create a new upload.
	uploadId = uuid.NewString()

	// Create file to save uploaded chunks
	file, err = os.OpenFile(filepath.Join(m.TmpDir, uploadId), os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create file to indicate incompleteness
	if err := os.WriteFile(filepath.Join(m.TmpDir, uploadId+IncompleteExt), nil, 0o644); err != nil {
		return err
	}

	uploadUrl := getUrl(r) + uploadId
	m.logger.Info("UploadCreationHandler", zap.String("uploadUrl", uploadUrl))
	w.Header().Set("Location", uploadUrl)

	// Respond with informational response, if interop version matches
	if r.Header.Get("Upload-Draft-Interop-Version") == InteropVersion {
		w.Header().Set("Upload-Draft-Interop-Version", InteropVersion)
		w.WriteHeader(104)
		w.Header().Del("Upload-Draft-Interop-Version")
	}

	// Copy request body to file
	_, err = io.Copy(file, r.Body)
	if err != nil {
		return err
	}

	// Obtain latest offset
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// Check if upload is done now.
	// Note: If there was an issue reading the request body, we will already
	// have errored out. So here we can assume the request body reading was successful.
	isComplete := getUploadComplete(r)
	if isComplete {
		// Remove file indicating incompleteness
		if err := os.Remove(filepath.Join(m.TmpDir, uploadId+IncompleteExt)); err != nil {
			return err
		}
	}

	setUploadHeaders(w, isComplete, offset)

	return nil
}

func (m *Middleware) OffsetRetrievingHandler(w http.ResponseWriter, r *http.Request) error {
	segments := strings.Split(r.URL.Path, "/")
	id := segments[len(segments)-1]

	file, exists, isComplete, offset, err := m.loadUpload(id)
	m.logger.Info("OffsetRetrievingHandler", zap.String("id", id), zap.Bool("exists", exists), zap.Bool("isComplete", isComplete), zap.Int64("offset", offset))
	if err != nil {
		return err
	}

	if !exists {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("upload not found\n"))
		return nil
	}
	defer file.Close()

	setUploadHeaders(w, isComplete, offset)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (m *Middleware) UploadAppendingHandler(w http.ResponseWriter, r *http.Request) error {
	segments := strings.Split(r.URL.Path, "/")
	id := segments[len(segments)-1]

	file, exists, complete_server, offset_server, err := m.loadUpload(id)
	if err != nil {
		return err
	}
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("upload not found\n"))
		return nil
	}
	defer file.Close()

	complete_client := !getUploadComplete(r)
	offset_client, ok := getUploadOffset(r)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid or missing Upload-Offset header\n"))
		return nil
	}

	if offset_server != offset_client {
		setUploadHeaders(w, complete_server, offset_server)
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte("mismatching Upload-Offset value\n"))
		return nil
	}

	if complete_server {
		setUploadHeaders(w, complete_server, offset_server)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("upload is already complete\n"))
		return nil
	}

	// r.Body is always non-nil
	n, err := io.Copy(file, r.Body)
	if err != nil {
		return err
	}

	offset_server += n

	if complete_client {
		complete_server = true
		if err := os.Remove(filepath.Join(m.TmpDir, id+IncompleteExt)); err != nil {
			return err
		}
	}

	setUploadHeaders(w, complete_server, offset_server)
	w.WriteHeader(http.StatusCreated)
	return nil
}

func (m *Middleware) loadUpload(id string) (file *os.File, exists bool, isComplete bool, offset int64, err error) {
	file, err = os.OpenFile(filepath.Join(m.TmpDir, id), os.O_WRONLY, 0o644)
	if errors.Is(err, os.ErrNotExist) {
		exists = false
		err = nil
		return // naked return
	}
	if err != nil {
		return
	}

	exists = true
	offset, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return
	}

	_, err = os.Stat(filepath.Join(m.TmpDir, id+IncompleteExt))
	if errors.Is(err, os.ErrNotExist) {
		isComplete = true
		err = nil
	}
	if err != nil {
		return
	}

	return
}

func setUploadHeaders(w http.ResponseWriter, isComplete bool, offset int64) {
	if isComplete {
		w.Header().Set("Upload-Complete", "?1")
	} else {
		w.Header().Set("Upload-Complete", "?0")
	}
	w.Header().Set("Upload-Offset", strconv.FormatInt(offset, 10))
}

func getUploadComplete(r *http.Request) bool {
	if r.Header.Get("Upload-Complete") == "?1" {
		return true
	} else {
		return false
	}
}

func getUploadOffset(r *http.Request) (int64, bool) {
	offset, err := strconv.Atoi(r.Header.Get("Upload-Offset"))
	if err != nil {
		return 0, false
	}
	return int64(offset), true
}

func getUrl(r *http.Request) string {
	protocol := "http"
	path := r.URL.Path
	if r.TLS != nil {
		protocol = "https"
	}
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	return protocol + "://" + r.Host + path
}
