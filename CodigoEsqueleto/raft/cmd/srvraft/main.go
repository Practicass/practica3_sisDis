package main

import (
	//"errors"
	"fmt"
	//"log"
	"net"
	"net/rpc"
	"os"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
	"strconv"
	//"time"
)

func main() {

	// obtener entero de indice de este nodo
	me, err := strconv.Atoi(os.Args[1])
	check.CheckError(err, "Main, mal numero entero de indice de nodo:")

	var nodos []rpctimeout.HostPort

	// Resto de argumento son los end points como strings
	// De todas la replicas-> pasarlos a HostPort
	for _, endPoint := range os.Args[2:] {
		nodos = append(nodos, rpctimeout.HostPort(endPoint))
	}

	canalAplicarOperacion := make(chan raft.AplicaOperacion, 1000)

	// Parte Servidor
	nr := raft.NuevoNodo(nodos, me, make(chan raft.AplicaOperacion, 1000))
	rpc.Register(nr)

	almacen := make(map[string]string)
	go realizarOperacion(almacen, canalAplicarOperacion)

	l, err := net.Listen("tcp", os.Args[2:][me])
	check.CheckError(err, "Main listen error:")

	for {
		rpc.Accept(l)

	}

}

func realizarOperacion(almacen map[string]string, canal chan raft.AplicaOperacion) {
    for {
        operacion := <-canal
        fmt.Println(operacion)
        if operacion.Operacion.Operacion == "leer" {
            operacion.Operacion.Valor = almacen[operacion.Operacion.Clave]
        } else if operacion.Operacion.Operacion == "escribir" {
            almacen[operacion.Operacion.Clave] = operacion.Operacion.Valor
            operacion.Operacion.Valor = "hecho"
        }
        canal <- operacion
    }
}
