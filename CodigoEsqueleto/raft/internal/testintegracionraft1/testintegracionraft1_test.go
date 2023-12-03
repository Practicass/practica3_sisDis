package testintegracionraft1

import (
	"fmt"
	"raft/internal/comun/check"

	//"log"
	//"crypto/rand"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
)

const (
	//hosts
	MAQUINA1 = "127.0.0.1"
	MAQUINA2 = "127.0.0.1"
	MAQUINA3 = "127.0.0.1"
	//MAQUINA1 = "192.168.3.18"
	//MAQUINA2 = "192.168.3.19"
	//MAQUINA3 = "192.168.3.20"

	//puertos
	PUERTOREPLICA1 = "29001"
	PUERTOREPLICA2 = "29002"
	PUERTOREPLICA3 = "29003"
	//PUERTOREPLICA1 = "31040"
	//PUERTOREPLICA2 = "31041"
	//PUERTOREPLICA3 = "31042"
	//nodos replicas
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "cmd/srvraft/main.go"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas

	PRIVKEYFILE = "id_rsa"
)

// PATH de los ejecutables de modulo golang de servicio Raft
var PATH string = filepath.Join(os.Getenv("HOME"), "Escritorio", "3", "sistDis", "practica4_sisDis", "CodigoEsqueleto", "raft")

//var PATH string = filepath.Join(os.Getenv("HOME"), "misc", "alumnos", "sd", "sd2324" , "a842255", "CodigoEsqueleto", "raft")

// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = "cd " + PATH + "; go run " + EXECREPLICA

// TEST primer rango
func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Run test sequence

	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T1:soloArranqueYparada",
		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })

	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

	// Test3: tenemos el primer primario correcto
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: Tres operaciones comprometidas en configuración estable
	t.Run("T4:tresOperacionesComprometidasEstable",
		func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })
}

// TEST primer rango
func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T6:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T7:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// ---------------------------------------------------------------------
// Operativa en configuracion de despliegue y pruebas asociadas
type configDespliegue struct {
	t           *testing.T
	conectados  []bool
	numReplicas int
	nodosRaft   []rpctimeout.HostPort
	cr          canalResultados

	lider int
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(t *testing.T, n int, nodosraft []string,
	conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.t = t
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)
	cfg.cr = make(canalResultados, 2000)

	return cfg
}

func (cfg *configDespliegue) stop() {
	//cfg.stopDistributedProcesses()

	time.Sleep(100 * time.Millisecond)

	cfg.cr.stop()
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se pone en marcha una replica ?? - 3 NODOS RAFT
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	//t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	cfg.t = t // Actualizar la estructura de datos de tests para errores

	// Poner en marcha replicas en remoto con un tiempo de espera incluido
	cfg.startDistributedProcesses()

	// Comprobar estado replica 0

	cfg.comprobarEstadoRemoto(0, 0, false, -1)

	// Comprobar estado replica 1
	cfg.comprobarEstadoRemoto(1, 0, false, -1)

	// Comprobar estado replica 2
	cfg.comprobarEstadoRemoto(2, 0, false, -1)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha - 3 NODOS RAFT
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	//t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Se ha elegido lider ?
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)

	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo - 3 NODOS RAFT
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	//t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	fmt.Printf("Lider inicial\n")
	cfg.pruebaUnLider(3)

	// Desconectar lider
	// ???
	fmt.Printf("Parar lider\n")
	cfg.pararLider()

	fmt.Printf("Comprobar nuevo lider\n")
	cfg.pruebaUnLider(3)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
	//time.Sleep(1000 * time.Millisecond)

}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	//t.Skip("SKIPPED tresOperacionesComprometidasEstable")

	
	// A completar ???
	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Se ha elegido lider ?
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)

	fmt.Printf("Sometiendo operaciones\n")
	cfg.pruebaComprometerOperacion(0, "escribir", "x", "operacion1", "hecho")
	cfg.pruebaComprometerOperacion(1, "escribir", "y", "operacion2", "hecho")
	cfg.pruebaComprometerOperacion(2, "leer", "x", "", "operacion1")
	//time.Sleep(1 * time.Second)

	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")

}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	//t.Skip("SKIPPED AcuerdoApesarDeSeguidor")

	// A completar ???
	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()
	// Comprometer una entrada
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)
	fmt.Printf("Sometiendo operaciones\n")
	fmt.Print(cfg.conectados)
	cfg.pruebaComprometerOperacion(0, "escribir", "x", "operacion10", "hecho")
	cfg.pruebaComprometerOperacion(1, "escribir", "y", "operacion2", "hecho")

	//  Obtener un lider y, a continuación desconectar una de los nodos Raft
	fmt.Println("Desconectando nodo aleatorio")
	cfg.pararSeguidorAleatorio()

	// Comprobar varios acuerdos con una réplica desconectada
	fmt.Printf("Sometiendo operaciones\n")
	cfg.pruebaComprometerOperacion(2, "escribir", "y", "operacion3", "hecho")
	cfg.pruebaComprometerOperacion(3, "leer", "y", "", "operacion2")

	// reconectar nodo Raft previamente desconectado y comprobar varios acuerdos
	cfg.reiniciarNodosDesconectados()
	cfg.pruebaComprometerOperacion(4, "escribir", "z", "operacion4", "hecho")
	cfg.pruebaComprometerOperacion(5, "leer", "x", "", "operacion1")
	cfg.pruebaComprometerOperacion(6, "escribir", "x", "operacion5", "hecho")

	cfg.stopDistributedProcesses()
	fmt.Println(".............", t.Name(), "Superado")
}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	//t.Skip("SKIPPED SinAcuerdoPorFallos")

	// A completar ???
	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Comprometer una entrada
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)
	fmt.Printf("Sometiendo operaciones\n")
	cfg.pruebaComprometerOperacion(0, "escribir", "x", "operacion1", "hecho")

	//  Obtener un lider y, a continuación desconectar 2 de los nodos Raft
	cfg.pararSeguidorAleatorio()
	cfg.pararSeguidorAleatorio()
	time.Sleep(1000 * time.Millisecond)
	// Comprobar varios acuerdos con 2 réplicas desconectada
	cfg.pruebaComprometerOperacionSinMayoria("escribir", "y", "operacion2")
	cfg.pruebaComprometerOperacionSinMayoria("escribir", "y", "operacion3")
	cfg.pruebaComprometerOperacionSinMayoria("escribir", "y", "")

	// reconectar lo2 nodos Raft  desconectados y probar varios acuerdos
	fmt.Printf("Reconectar nodos y probar a someter operaciones\n")
	cfg.reiniciarNodosDesconectados()
	cfg.pruebaComprometerOperacion(4, "escribir", "z", "operacion4", "hecho")
	cfg.pruebaComprometerOperacion(5, "leer", "x", "", "operacion1")
	cfg.pruebaComprometerOperacion(6, "escribir", "x", "operacion5", "hecho")

	cfg.stopDistributedProcesses()
	fmt.Println(".............", t.Name(), "Superado")
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	//t.Skip("SKIPPED SometerConcurrentementeOperaciones")

	// un bucle para estabilizar la ejecucion
	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Obtener un lider y, a continuación someter una operacion
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)
	fmt.Printf("Sometiendo operacion\n")
	cfg.pruebaComprometerOperacion(0, "escribir", "x", "operacion1", "hecho")
	// Someter 5  operaciones concurrentes
	go cfg.someterOperacion("escribir", "y", "operacion2")
	go cfg.someterOperacion("escribir", "y", "operacion3")
	go cfg.someterOperacion("leer", "y", "")
	go cfg.someterOperacion("escribir", "z", "operacion4")
	go cfg.someterOperacion("leer", "y", "")
	// Comprobar estados de nodos Raft, sobre todo
	// el avance del mandato en curso e indice de registro de cada uno
	// que debe ser identico entre ellos
	time.Sleep(2 * time.Second)
	cfg.comprobarEstadosIguales(5)
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")

}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(250 * time.Millisecond)
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {

				if _, mandato, eslider, _ := cfg.obtenerEstadoRemoto(i); eslider {
					mapaLideres[mandato] = append(mapaLideres[mandato], i)
					fmt.Println("LIDER")
				}

			}
		}

		ultimoMandatoConLider := -1
		for mandato, lideres := range mapaLideres {
			if len(lideres) > 1 {
				cfg.t.Fatalf("mandato %d tiene %d (>1) lideres",
					mandato, len(lideres))
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {
			lider := mapaLideres[ultimoMandatoConLider][0]
			cfg.lider = lider
			fmt.Println(lider + 1)
			return lider // Termina

		}
	}
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")

	return -1 // Termina
}

func (cfg *configDespliegue) obtenerEstadoRemoto(
	indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, 10*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
	//cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)

	for i, endPoint := range cfg.nodosRaft {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(i)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)
			cfg.conectados[i] = true
		// dar tiempo para se establezcan las replicas
		//time.Sleep(500 * time.Millisecond)

	}

	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(1 * time.Second) //PARA TEST 1

}

func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for i, endPoint := range cfg.nodosRaft {
		fmt.Print(cfg.conectados)
		if cfg.conectados[i] == true {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 10*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC Para nodo")
			cfg.conectados[i] = false
		}

	}
}

// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int,
	mandatoDeseado int, esLiderDeseado bool, IdLiderDeseado int) {
	idNodo, mandato, esLider, idLider := cfg.obtenerEstadoRemoto(idNodoDeseado)

	cfg.t.Log("Estado replica 0: ", idNodo, mandato, esLider, idLider, "\n")

	if idNodo != idNodoDeseado || mandato != mandatoDeseado ||
		esLider != esLiderDeseado || idLider != IdLiderDeseado {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			idNodoDeseado, cfg.t.Name())
	}

}

func (cfg *configDespliegue) pararLider() {
	var reply raft.Vacio
	for i, endPoint := range cfg.nodosRaft {
		if i == cfg.lider {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo", raft.Vacio{},
				&reply, 10*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC ParaNodo")
			cfg.conectados[i] = false
		}
	}
}

func (cfg *configDespliegue) pruebaComprometerOperacion(indice int,
	operacion string, clave string, valor string, respuesta string) {
	var reply raft.ResultadoRemoto

	err := cfg.nodosRaft[cfg.lider].CallTimeout("NodoRaft.SometerOperacionRaft",
		raft.TipoOperacion{operacion, clave, valor}, &reply, 2000*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC SometerOperacionRaft")
	cfg.lider = reply.IdLider
	fmt.Print(reply, indice, valor)
	if reply.IndiceRegistro != indice || reply.ValorADevolver != valor {
		cfg.t.Fatalf("Operacion comprometida incorrectamente en %d en subtest %s",
			indice, cfg.t.Name())
	}
}

func (cfg *configDespliegue) reiniciarNodosDesconectados() {
	//cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)

	for i, endPoint := range cfg.nodosRaft {
		if !cfg.conectados[i] {
			despliegue.ExecMutipleHosts(EXECREPLICACMD+
				" "+strconv.Itoa(i)+" "+
				rpctimeout.HostPortArrayToString(cfg.nodosRaft),
				[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)
			cfg.conectados[i] = true
			// dar tiempo para se establezcan las replicas
			//time.Sleep(500 * time.Millisecond)
		}
	}

	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(1000 * time.Millisecond) //PARA TEST 1

}

func (cfg *configDespliegue) pararSeguidorAleatorio() {
	var reply raft.Vacio
	var aux int
	aux = rand.Intn(len(cfg.nodosRaft))
	for aux == cfg.lider || !cfg.conectados[aux] {
		aux = rand.Intn(len(cfg.nodosRaft))
	}
	for i, endPoint := range cfg.nodosRaft {
		if i == aux {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo", raft.Vacio{},
				&reply, 10*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC ParaNodo")
			cfg.conectados[i] = false

			fmt.Println(i + 1)
		}
	}
}

func (cfg *configDespliegue) pruebaComprometerOperacionSinMayoria(operacion string,
	clave string, valor string) {
	var reply raft.ResultadoRemoto

	err := cfg.nodosRaft[cfg.lider].CallTimeout("NodoRaft.SometerOperacionRaft",
		raft.TipoOperacion{operacion, clave, valor}, &reply, 2000*time.Millisecond)

	if err == nil {
		cfg.t.Fatalf("Se ha conseguido acuerdo sin mayoria %s",
			cfg.t.Name())
	} else {
		fmt.Println("No se ha llegado a un acuerdo")
	}
}


func (cfg *configDespliegue) someterOperacion(operacion string,
	clave string, valor string) {
	var reply raft.ResultadoRemoto

	err := cfg.nodosRaft[cfg.lider].CallTimeout("NodoRaft.SometerOperacionRaft",
		raft.TipoOperacion{operacion, clave, valor}, &reply, 2000*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC SometerOperacionRaft")
	cfg.lider = reply.IdLider

}

func (cfg *configDespliegue) comprobarEstadosIguales(indiceEsperado int) {
	indices := make([]int, cfg.numReplicas)
	mandatos := make([]int, cfg.numReplicas)
	for i, _ := range cfg.nodosRaft {
		var reply raft.EstadoLog
		err := cfg.nodosRaft[cfg.lider].CallTimeout("NodoRaft.ObtenerEstadoLog",
			raft.Vacio{}, &reply, 2000*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC SometerOperacionRaft")
		indices[i], mandatos[i] = reply.Indice, reply.Mandato
	}
	if indices[0] != indiceEsperado {
		cfg.t.Fatalf("Avance de indice de registro incorrecto en subtest %s",
			cfg.t.Name())
	}
	for i := 1; i < cfg.numReplicas; i++ {
		if indices[0] != indices[i] || mandatos[0] != mandatos[i] {
			cfg.t.Fatalf(
				"No coincide el estado de todos los nodos en subtest %s",
				cfg.t.Name())
		}
	}

}