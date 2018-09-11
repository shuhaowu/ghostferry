package main

import (
	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/test/integration/go/integrationferry"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.DebugLevel)

	config := &ghostferry.Config{
		Source: ghostferry.DatabaseConfig{
			Host:      "127.0.0.1",
			Port:      uint16(29291),
			User:      "root",
			Pass:      "",
			Collation: "utf8mb4_unicode_ci",
			Params: map[string]string{
				"charset": "utf8mb4",
			},
		},

		Target: ghostferry.DatabaseConfig{
			Host:      "127.0.0.1",
			Port:      uint16(29292),
			User:      "root",
			Pass:      "",
			Collation: "utf8mb4_unicode_ci",
			Params: map[string]string{
				"charset": "utf8mb4",
			},
		},

		AutomaticCutover: true,
		TableFilter: &testhelpers.TestTableFilter{
			DbsFunc:    testhelpers.DbApplicabilityFilter([]string{"gftest"}),
			TablesFunc: nil,
		},
	}

	err := config.ValidateConfig()
	if err != nil {
		panic(err)
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
