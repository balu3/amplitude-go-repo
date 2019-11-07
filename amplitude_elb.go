package main

import (
	"test_amplitude_go/internal/event"
	// "updated_amplitude_go/internal/logger"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
	// "fmt"
	"github.com/gorilla/mux"
	// "github.com/nsqio/go-nsq"
	

)


func amplitudeResponse(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html;charset=utf-8")
		w.Header().Set("access-control-allow-origin", "*")
		w.Header().Set("access-control-allow-methods", "GET, POST")
		w.Header().Set("strict-transport-security", "max-age=15768000")
		next.ServeHTTP(w, r)
	}
}

func writeError(w http.ResponseWriter, r *http.Request, error string, code int) {
	log.Printf("Error: %s %s %s %d", r.Method, r.URL, error, code)
	http.Error(w, error, code)
}

// var producer *nsq.Producer

// func init() {
// 	var err error
// 	config := nsq.NewConfig()
// 	producer, err = nsq.NewProducer("localhost:4150", config)
// 	if err != nil {
// 		log.Println("Exception: unable to start producer")
// 	}

// 	producer.SetLogger(&logger.NoopNSQLogger{}, nsq.LogLevelError)

// 	started := false
// 	for i := 0; i < 10; i++ {
// 		err = producer.Ping()
// 		if err != nil {
// 			log.Println("Exception: unable to ping")
// 		} else {
// 			started = true
// 			break
// 		}
// 		time.Sleep(10 * time.Second)
// 	}

// 	if !started {
// 		log.Fatalln("unable to start producer, fatal")
// 	}

// 	log.Println("started nsq producer")
// }

func handleAmplitude(w http.ResponseWriter, r *http.Request) {
	
	elbEvent := event.NewELB(r)
	jsonData, err := json.Marshal(elbEvent)
	if err != nil {
		writeError(w, r, "invalid_json", 500)
		return
	}
	if len(elbEvent.Body) == 0 {
		http.Error(w, "no_body", 404)
		return
	}

	// async function to insert raw-data to kinesis stream
	go func() {
		kinesisHandler(w, r, jsonData)
	}()
	// err = producer.Publish("elb_incoming_topic", jsonData)
	// if err != nil {
	// 	writeError(w, r, "unable_to_post", 500)
	// }

	// _, err = w.Write([]byte("success"))
	// if err != nil {
	// 	writeError(w, r, "unable_to_write", 500)
	// 	return
	// }
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	handleAmplitude(w, r)
}

func handleHTTPAPI(w http.ResponseWriter, r *http.Request) {
	handleAmplitude(w, r)
}

func handleIdentify(w http.ResponseWriter, r *http.Request) {
	handleAmplitude(w, r)
}

func handleTest(w http.ResponseWriter, r *http.Request) {
	_, err := w.Write([]byte("test"))
	if err != nil {
		writeError(w, r, "unable_to_write", 500)
		return
	}
}

func handleError(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, "error", 500)
}

func notFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, "not_found", 404)
}

func notAllowed(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, "not_allowed", 405)
}

func main() {
	
	
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	r := mux.NewRouter()
	r.HandleFunc("/", amplitudeResponse(handleRoot)).Methods("POST", "GET")
	r.HandleFunc("/httpapi", amplitudeResponse(handleHTTPAPI)).Methods("POST")
	r.HandleFunc("/identify", amplitudeResponse(handleIdentify)).Methods("POST")
	r.HandleFunc("/test", amplitudeResponse(handleTest)).Methods("POST", "GET")
	r.HandleFunc("/error", amplitudeResponse(handleError))
	r.NotFoundHandler = http.HandlerFunc(amplitudeResponse(notFound))
	r.MethodNotAllowedHandler = http.HandlerFunc(amplitudeResponse(notAllowed))

	addr := ":8088"
	h := &http.Server{Addr: addr, Handler: r}

	go func() {
		log.Printf("starting server....  %v %v \n", runtime.GOMAXPROCS(0), runtime.NumCPU())
		err := h.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("fatal error running the server %v \n", err)
		}
	}()

	<-termChan // wait for signal

	log.Printf("ending server.... \n")
	timeout := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log.Printf("shutdown with timeout: %s\n", timeout)

	err := h.Shutdown(ctx)
	if err != nil {
		log.Printf("error ending server.... %v \n", err)
	}

	log.Printf("stopping producer \n")
	// producer.Stop()

	//stops the kinesis producer
	stopKinesisProducer()
	
	log.Printf("done \n")
}
