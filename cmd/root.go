package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"os"

	gorillahandlers "github.com/gorilla/handlers"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/journald"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/net/webdav"

	"github.com/cartabinaria/fileseeker/fs"
	"github.com/cartabinaria/fileseeker/handlers"
	"github.com/cartabinaria/fileseeker/listfs"
	"github.com/cartabinaria/fileseeker/telemetry"
)

type configType []struct {
	Years []struct {
		Teachings []struct {
			Url string `json:"url"`
		} `json:"teachings"`
	} `json:"years"`
}

const (
	serviceName = "fileseeker"
	serviceVer  = "0.1.0"
)

var (
	RootCmd = &cobra.Command{
		Use:   serviceName,
		Short: "a webdav proxy for CartaBinaria",
		Long: "a webdav server that serves files " +
			"from a statik.json file tree, as produced by statik.",
		Run: Execute,
	}
	configFile    string
	grpcEndpoint  string
	grpcSecure    bool
	basePath      string
	addr          string
	proxyEnabled  bool
	humanReadable bool
	debug         bool
	logJournald   bool
)

func init() {
	RootCmd.Flags().StringVarP(&configFile, "config", "c", "config/courses.json", "path to config file")
	RootCmd.Flags().StringVar(&grpcEndpoint, "otel", "", "endpoint of the grpc server for OpenTelemetry")
	RootCmd.Flags().BoolVar(&grpcSecure, "otelsecure", false, "use secure connection for OpenTelemetry")
	RootCmd.Flags().StringVarP(&addr, "addr", "a", "localhost:8080", "address to listen on")
	RootCmd.Flags().BoolVar(&proxyEnabled, "proxy", false, "enable proxy handling")
	RootCmd.Flags().BoolVar(&humanReadable, "human", false, "enable human readable output")
	RootCmd.Flags().BoolVarP(&debug, "debug", "d", false, "enable debug output")
	RootCmd.Flags().BoolVar(&logJournald, "journald", false, "enable logJournald output")

	RootCmd.Flags().StringVarP(&basePath, "basepath", "b", "", "base path for the static files")
	_ = RootCmd.MarkFlagRequired("basepath")
}

func Execute(*cobra.Command, []string) {

	if humanReadable && logJournald {
		log.Fatal().Msg("--human and --journald are incompatible")
	} else if humanReadable {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	} else if logJournald {
		log.Logger = log.Output(journald.NewJournalDWriter())
	}

	if debug {
		log.Logger = log.Level(zerolog.DebugLevel)
	} else {
		log.Logger = log.Level(zerolog.InfoLevel)
	}

	// Add trailing slash to base path if not present
	if basePath[len(basePath)-1] != '/' {
		basePath += "/"
	}

	var config configType
	configStr, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatal().Err(err).Str("file", configFile).Msg("error reading config file")
	}

	err = json.Unmarshal(configStr, &config)
	if err != nil {
		log.Fatal().Err(err).Str("file", configFile).Msg("error parsing config file")
	}

	// Setup telemetry
	if grpcEndpoint != "" {
		log.Info().Str("endpoint", grpcEndpoint).Bool("secure", grpcSecure).Msg("setting up telemetry")
		shutdown, err := telemetry.SetupOTelSDK(context.Background(),
			serviceName, serviceVer,
			grpcEndpoint, grpcSecure)
		if err != nil {
			log.Fatal().Err(err).Msg("error while setting up telemetry")
		}

		defer func() {
			if err := shutdown(context.Background()); err != nil {
				log.Error().Err(err).Msg("error while shutting down telemetry")
			}
		}()
	}

	logger := handlers.ZerologWebdavLogger(log.Logger, zerolog.InfoLevel)

	mux := http.NewServeMux()

	teachings := make([]string, 0, len(config))
	for _, course := range config {
		for _, year := range course.Years {
			for _, teaching := range year.Teachings {
				url := teaching.Url
				teachings = append(teachings, url)
				log.Info().Str("url", url).Msg("creating handle")
				handleTeaching(mux, url, logger)
			}
		}
	}

	log.Info().Str("url", "/").Msg("creating handle")
	mux.Handle("/", &webdav.Handler{
		FileSystem: listfs.NewListFS(teachings),
		LockSystem: webdav.NewMemLS(),
		Logger:     logger,
	})

	log.Info().Msg("creating logging handler")

	handler := otelhttp.NewHandler(mux, "http-server")

	if proxyEnabled {
		log.Warn().Msg("proxy handling enabled. If you are not behind a proxy, set proxy option to false!")
		handler = gorillahandlers.ProxyHeaders(handler)
	}

	log.Info().Str("addr", addr).Msg("starting server")
	err = http.ListenAndServe(addr, handler)
	if err != nil {
		log.Fatal().Err(err).Msg("error while serving")
	}
}

func handleTeaching(mux *http.ServeMux, url string, logger func(req *http.Request, err error)) {
	statikFS, err := fs.NewStatikFS(basePath + url)
	if err != nil {
		log.Fatal().Err(err).Str("url", url).Msg("error creating statik fs")
	}

	handler := &webdav.Handler{
		Prefix:     "/" + url,
		FileSystem: statikFS,
		LockSystem: webdav.NewMemLS(),
		Logger:     logger,
	}

	mux.Handle("/"+url+"/", handler)
}
