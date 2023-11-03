package main

import (
	"github.com/rs/zerolog/log"

	"github.com/csunibo/fileseeker/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("error executing root command")
	}
}
