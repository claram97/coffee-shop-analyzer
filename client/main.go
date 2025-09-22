package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/op/go-logging"
	"github.com/spf13/viper"

	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/common"
)

var log = logging.MustGetLogger("log")

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("orchestrator", "address")
	v.BindEnv("log", "level")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

// PrintConfig Print all the configuration parameters of the program.
// For debugging purposes only
func PrintConfig(v *viper.Viper) {
	log.Infof("action: config | result: success | client_id: %s | orchestrator_address: %s | log_level: %s",
		v.GetString("id"),
		v.GetString("orchestrator.address"),
		v.GetString("log.level"),
	)
}

func main() {
	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
		return
	}

	// Print program config with debugging purposes
	PrintConfig(v)

	var handler common.TableRowHandler
	var opCode byte

	tableType := "users"

	switch tableType {
	case "bets":
		handler = common.BetHandler{}
		opCode = common.OpCodeNewBets
		log.Infof("Client configured to send BETS")
	case "menu_items":
		handler = common.MenuItemHandler{}
		opCode = common.OpCodeNewMenuItems
		log.Infof("Client configured to send MENU ITEMS")
	case "payment_methods":
		handler = common.PaymentMethodHandler{}
		opCode = common.OpCodeNewPaymentMethods
		log.Infof("Client configured to send PAYMENT METHODS")
	case "stores":
		handler = common.StoreHandler{}
		opCode = common.OpCodeNewStores
		log.Infof("Client configured to send STORES")
	case "transaction_items":
		handler = common.TransactionItemHandler{}
		opCode = common.OpCodeNewTransactionItems
		log.Infof("Client configured to send TRANSACTION ITEMS")
	case "transactions":
		handler = common.TransactionHandler{}
		opCode = common.OpCodeNewTransaction
		log.Infof("Client configured to send TRANSACTIONS")
	case "users":
		handler = common.UserHandler{}
		opCode = common.OpCodeNewUsers
		log.Infof("Client configured to send USERS")
	case "vouchers":
		handler = common.VoucherHandler{}
		opCode = common.OpCodeNewVouchers
		log.Infof("Client configured to send VOUCHERS")
	default:
		log.Criticalf("Invalid table.type configured: '%s'. Valid options are bets, menu_items, payment_methods, stores, transaction_items, transactions, users, vouchers")
		return
	}

	clientConfig := common.ClientConfig{
		ServerAddress: v.GetString("orchestrator.address"),
		ID:            v.GetString("id"),
		BetsFilePath:  "./data/users/users_202506.csv",
		BatchLimit:    v.GetInt32("batch.maxAmount"),
	}

	betClient := common.NewClient(clientConfig, handler, opCode)

	betClient.SendBets()
}
