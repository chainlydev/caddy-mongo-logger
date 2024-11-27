package mongo_log

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(MongoLog{})
	caddy.RegisterModule(MongoReqId{})
	httpcaddyfile.RegisterHandlerDirective("mongo_request_id", parseCaddyfile)
}

type MongoReqId struct {
	logger *zap.Logger
	Header string `json:"header,omitempty"`
}

func (m *MongoReqId) Provision(ctx caddy.Context) error {
	m.logger = ctx.Logger(m)
	return nil
}
func (m MongoReqId) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	repl := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer)
	uid, _ := uuid.NewV7()

	id := uid.String()
	repl.Set("http.mongo_request_id", id)

	data, _ := io.ReadAll(r.Body)
	dataResp, _ := io.ReadAll(r.Response.Body)
	m.logger.Debug("mongolog", zap.String("req_id", id), zap.String("req_body", string(data)), zap.String("resp_body", string(dataResp)))
	w.Header().Add("X-Request-Id", id)
	return next.ServeHTTP(w, r)
}

// CaddyModule implements caddy.Module.
func (m MongoReqId) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.mongo_request_id",
		New: func() caddy.Module { return new(MongoLog) },
	}
}
func (m *MongoReqId) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	return nil
}
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	m := new(MongoReqId)
	err := m.UnmarshalCaddyfile(h.Dispenser)
	if err != nil {
		return nil, err
	}

	return m, nil
}

type MongoLog struct {
	MongoUri   string            `json:"mongoUri,omitempty"`
	Database   string            `json:"database,omitempty"`
	Collection string            `json:"collection,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`

	logger *zap.Logger
}

// CaddyModule returns the Caddy module information.
func (MongoLog) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.logging.writers.mongo_log",
		New: func() caddy.Module { return new(MongoLog) },
	}
}

func (l *MongoLog) String() string {
	return "mongo_log"
}

func (l *MongoLog) WriterKey() string {
	return "mongo_log"
}

func (l *MongoLog) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	// Consumes the option name
	if !d.NextArg() {
		return d.ArgErr()
	}

	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "mongoUri":
			if !d.NextArg() {
				return d.ArgErr()
			}

			l.MongoUri = d.Val()
		case "collection":
			if !d.NextArg() {
				return d.ArgErr()
			}

			l.Collection = d.Val()

		case "database":
			if !d.NextArg() {
				return d.ArgErr()
			}

			l.Database = d.Val()

		case "tags":
			tags := map[string]string{}
			for nesting_tags := d.Nesting(); d.NextBlock(nesting_tags); {
				key := d.Val()

				if !d.NextArg() {
					return d.ArgErr()
				}

				tags[key] = d.Val()
			}
			l.Tags = tags
		}
	}

	return nil
}

func (l *MongoLog) OpenWriter() (io.WriteCloser, error) {
	writer := &mongoWriter{
		logger: l.logger,
	}

	go func() {
		writer.Open(l)
	}()

	return writer, nil
}

func (l *MongoLog) Provision(ctx caddy.Context) error {
	l.logger = ctx.Logger(l)

	return nil
}

func (l *MongoLog) Validate() error {
	if l.MongoUri == "" {
		return fmt.Errorf("NO HOST SET")
	}

	if l.Database == "" {
		return fmt.Errorf("NO DATABASE SET")
	}

	if l.Collection == "" {
		return fmt.Errorf("NO COLLECTION SET")
	}

	if l.Tags == nil {
		l.Tags = map[string]string{}
	}

	return nil
}

func flatten(m map[string]interface{}, fields map[string]interface{}, prefix string) map[string]interface{} {
	for k, v := range m {
		key := prefix + k

		if v2, ok := v.(map[string]interface{}); ok {
			flatten(v2, fields, key+"_")
		} else {
			fields[key] = v
		}
	}
	return m
}

type mongoWriter struct {
	logger      *zap.Logger
	measurement string
	tags        map[string]string
	client      *mongo.Client
	collection  *mongo.Collection
}

func (mWrite *mongoWriter) Write(p []byte) (n int, err error) {

	f := map[string]interface{}{}
	if err := json.Unmarshal(p, &f); err != nil {
		mWrite.logger.Error("Unmarshal failed on log", zap.Error((err)))
	}

	mWrite.collection.InsertOne(context.Background(), bson.M{
		"tags":     "",
		"metadata": f,
		"date":     primitive.NewDateTimeFromTime(time.Now()),
	})

	return
}

func (mWrite *mongoWriter) Close() error {
	mWrite.client.Disconnect(context.Background())
	return nil
}

func (mWrite *mongoWriter) Open(i *MongoLog) error {

	con, err := mongo.Connect(context.Background(), options.Client().ApplyURI(i.MongoUri))
	if err != nil {
		return err
	}
	mWrite.client = con
	mWrite.collection = con.Database(i.Database).Collection(i.Collection)
	mWrite.tags = i.Tags

	return nil
}

// Interface guards.
var (
	_ caddy.Provisioner           = (*MongoLog)(nil)
	_ caddy.Provisioner           = (*MongoReqId)(nil)
	_ caddy.Provisioner           = (*MongoReqId)(nil)
	_ caddyhttp.MiddlewareHandler = (*MongoReqId)(nil)
	_ caddyfile.Unmarshaler       = (*MongoReqId)(nil)
	_ caddy.Validator             = (*MongoLog)(nil)
	_ caddy.WriterOpener          = (*MongoLog)(nil)
	_ caddyfile.Unmarshaler       = (*MongoLog)(nil)
)
