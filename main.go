package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/accept-nano/accept-nano/internal/hub"
	"github.com/accept-nano/accept-nano/internal/nano"
	"github.com/accept-nano/accept-nano/internal/price"
	"github.com/accept-nano/accept-nano/internal/subscriber"
	"github.com/cenkalti/log"
	"github.com/ulule/limiter/v3"
	"github.com/ulule/limiter/v3/drivers/store/memory"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// These variables are set by goreleaser on build.
var (
	version = "0.0.0"
	commit  = ""
	date    = ""
)

var (
	generateSeed      = flag.Bool("seed", false, "generate a seed and exit")
	configPath        = flag.String("config", "config.toml", "config file path")
	versionFlag       = flag.Bool("version", false, "display version and exit")
	config            Config
	userclient        *mongo.Client
	collection        *mongo.Collection
	server            http.Server
	rateLimiter       *limiter.Limiter
	node              *nano.Node
	worknode          *nano.Node
	stopCheckPayments = make(chan struct{})
	checkPaymentWG    sync.WaitGroup
	verifications     hub.Hub
	priceAPI          *price.API
	subs              *subscriber.Subscriber
)

func versionString() string {
	const shaLen = 7
	if len(commit) > shaLen {
		commit = commit[:shaLen]
	}
	return fmt.Sprintf("%s (%s) [%s]", version, commit, date)
}

func ConnectMongoDB() (*mongo.Client, error) {

	if config.MongoDBConnectURI == "" {
		return nil, fmt.Errorf("no connection string provided in config")
	}

	log.Debugln("Connecting to DB")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// user Connection database

	// Set client options
	clientOptions := options.Client().ApplyURI(config.MongoDBConnectURI)

	// Connect to MongoDB
	userclient, err := mongo.Connect(ctx, clientOptions)

	if err != nil {
		return nil, err
	}

	// Check the connection
	err = userclient.Ping(ctx, nil)

	if err != nil {
		return nil, err
	}

	log.Debugln("Connected to user MongoDB!")

	return userclient, err

}

func main() {
	flag.Parse()

	if *versionFlag {
		fmt.Println(versionString())
		return
	}

	if *generateSeed {
		seed, err := NewSeed()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(seed)
		return
	}

	err := config.Read()
	if err != nil {
		log.Fatal(err)
	}

	if config.EnableDebugLog {
		log.SetLevel(log.DEBUG)
	}

	if config.CoinmarketcapAPIKey == "" {
		log.Warning("empty CoinmarketcapAPIKey in config, fiat conversions will not work")
	}

	userclient, err = ConnectMongoDB()
	if err != nil {
		log.Fatalln("error:", err)
		return
	}
	collection = userclient.Database(config.PaymentsDBName).Collection(config.PaymentsCollectionName)

	rate, err := limiter.NewRateFromFormatted(config.RateLimit)
	if err != nil {
		log.Fatal(err)
	}

	rateLimiter = limiter.New(memory.NewStore(), rate, limiter.WithTrustForwardHeader(true))
	node = nano.New(config.NodeURL, config.NodeTimeout, config.NodeAuthorizationHeader)
	if config.WorkNodeURL != "" {
		worknode = nano.New(config.WorkNodeURL, config.NodeTimeout, config.NodeAuthorizationHeader)
	}
	notificationClient.Timeout = config.NotificationRequestTimeout
	priceAPI = price.NewAPI(config.CoinmarketcapAPIKey, config.CoinmarketcapRequestTimeout, config.CoinmarketcapCacheDuration)

	// Check existing payments.
	payments, err := LoadActivePayments()
	if err != nil {
		log.Fatal(err)
	}
	log.Debugln("Existing payments:", len(payments))
	for _, p := range payments {
		p.StartChecking()
	}

	if !config.DisableWebsocket && config.NodeWebsocketURL != "" {
		subs = subscriber.New(config.NodeWebsocketURL, config.NodeWebsocketHandshakeTimeout, config.NodeWebsocketWriteTimeout, config.NodeWebsocketAckTimeout, config.NodeWebsocketKeepAlivePeriod)
		go subs.Run()
		go runChecker()
	}

	go runServer()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	close(stopCheckPayments)

	shutdownTimeout := config.ShutdownTimeout
	log.Noticeln("shutting down with timeout:", shutdownTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		log.Errorln("shutdown error:", err)
	}

	checkPaymentWG.Wait()
}

func runChecker() {
	for account := range subs.Confirmations {
		p, err := LoadPayment(account)
		if err == errPaymentNotFound {
			continue
		}
		if err != nil {
			log.Errorf("cannot load payment: %s", err.Error())
			continue
		}
		log.Debugf("received confirmation from websocket, checking account: %s", account)
		go p.checkOnce()
	}
}
