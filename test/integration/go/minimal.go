package main

import (
	"os"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/test/integration/go/integrationferry"
	"github.com/sirupsen/logrus"
)

func enableIterativeVerifier(config *ghostferry.Config) *ghostferry.Config {
	config.VerifierType = ghostferry.VerifierTypeIterative
	config.IterativeVerifierConfig = ghostferry.IterativeVerifierConfig{
		Concurrency: 2,
	}

	return config
}

func main() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.DebugLevel)

	config, err := integrationferry.NewStandardConfig()
	if err != nil {
		panic(err)
	}

	if os.Getenv("GHOSTFERRY_ITERATIVE_VERIFIER") != "" {
		config = enableIterativeVerifier(config)
	}

	f := &integrationferry.IntegrationFerry{
		Ferry: &ghostferry.Ferry{
			Config: config,
		},
	}

	err = f.Main()
	if err != nil {
		panic(err)
	}
}
