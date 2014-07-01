package main

import (
    "log"
    "net"
    "net/http"
    "bytes"
    "io"

    "code.google.com/p/go-uuid/uuid"
    "github.com/docopt/docopt-go"
)

func main() {
      usage := `tcpchunk: Listen on a TCP address and forward received data to an HTTP endpoint.
  If a non-200 response is returned, the connection is torn down.
  tcpchunk will POST to <drain_url>/<connection_id>.

Usage:
  tcpchunk <listen_address> <drain_url>

Options:
  -h --help     Show this screen.
`

  arguments, _ := docopt.Parse(usage, nil, true, "Tcpchunk v0", false)

  addr, ok := arguments["<listen_address>"].(string)
  if ok != true {
    log.Fatal("error while parsing options")
  }

  drain_url, ok := arguments["<drain_url>"].(string)
  if ok != true {
    log.Fatal("error while parsing options")
  }

  ln, err := net.Listen("tcp", addr)
  if err != nil {
    log.Fatal(err)
  }

  for {
    conn, err := ln.Accept()
    if err != nil {
      log.Println(err)
      continue
    }
    go handleConnection(conn, drain_url)
  }

}

const Bufsize = 4096

func handleConnection(conn net.Conn, drainBaseUrl string) {
  defer conn.Close()
  id := uuid.New()
  drain := drainBaseUrl+"/"+id
  log.Println("Connection from",conn.RemoteAddr(),"on",conn.LocalAddr(),"with id",id)
  buf := make([]byte, Bufsize)

  for {
    n, err := conn.Read(buf)
    if err == io.EOF {
      log.Println("Connection from",conn.RemoteAddr(),"closed.")
      break
    }

    if err != nil {
      log.Println("Error while reading from",conn.RemoteAddr(),":",err)
      break
    }

    resp, err := http.Post(drain, "application/octet-stream", bytes.NewReader(buf[0:n]))
    if err != nil {
      log.Println("Error while sending chunk:",err)
      break
    }

    if resp.StatusCode != 200 {
      log.Println("Got code",resp.StatusCode,"from",drain,"-- tearing down connection.")
      log.Println("%#v", resp)
      break
    }
  }
}
