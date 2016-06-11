// Package remoteworkers provides features for executing the same job on all the workers
// and collects results from workers.
//
// Workers must connect to the server with websockets and keep the connection active.
// When the server is down, workers tries reconnecting to the server.
//
// When the server receives a job from a client, the server sends the job to all
// workers and receives the result. When the server receives results from all the
// workers, the server sends the results to the client.
//
// When connections are lost for some workers, the server sends a job to only workers
// whose connection is active. Also when a connection is lost after sending a job
// to a worker, the server does not wait for the worker. When the server receives
// results from all active workers, the server sends results to the client.
package remoteworkers
