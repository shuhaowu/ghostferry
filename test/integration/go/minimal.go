package main

import (
	"encoding/json"
	"io/ioutil"
	"os"

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

		DumpStateToStdoutOnSignal: true,
	}

	resumeStateJSON, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}

	if len(resumeStateJSON) > 0 {
		config.StateToResumeFrom = &ghostferry.SerializableState{}
		err = json.Unmarshal(resumeStateJSON, config.StateToResumeFrom)
		if err != nil {
			panic(err)
		}
	}

	err = config.ValidateConfig()
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
