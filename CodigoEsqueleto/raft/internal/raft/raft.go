// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion


import (
	"fmt"
	"io"
	"log"
	"os"
	//"crypto/rand"
	"sync"
	"time"
	//"net/rpc"
	"math/rand"

	"raft/internal/comun/rpctimeout"
)


const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = false

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"
)

type TipoOperacion struct {
	Operacion string  // La operaciones posibles son "leer" y "escribir"
	Clave string
	Valor string    // en el caso de la lectura Valor = ""
}


// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados 
type AplicaOperacion struct {
	Indice int  // en la entrada de registro
	Operacion TipoOperacion
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
//
type NodoRaft struct {
	Mux   sync.Mutex       // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos []rpctimeout.HostPort
	Yo    int           // indice de este nodos en campo array "nodos"
	IdLider int
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	// Vuestros datos aqui.

	Rol string
	FollowerChannel chan bool
	HearbeatChannel chan bool
	MyVotes int
	LeaderChannel chan bool
	Replies int
	OperacionChannel chan AplicaOperacion
	Committed chan string

	// mirar figura 2 para descripción del estado que debe mantenre un nodo Raft
	CurrentTerm int
	VotedFor int
	Log[] Entry
	CommitIndex int
	LastApplied int
	NextIndex[] int
	MatchIndex[] int
}

type Entry struct{
	Indice int
	Mandato int
	Operacion TipoOperacion
}



// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []rpctimeout.HostPort, yo int, 
						canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = -1
	nr.Rol = "follower"
	nr.FollowerChannel = make(chan bool)
	nr.HearbeatChannel = make(chan bool)
	nr.MyVotes = 0
	nr.LeaderChannel = make(chan bool)
	nr.Replies = 0
	nr.OperacionChannel = canalAplicarOperacion
	nr.Committed = make(chan string)

	// mirar figura 2 para descripción del estado que debe mantenre un nodo Raft
	nr.CurrentTerm = 0
	nr.VotedFor = -1
	nr.CommitIndex = -1
	nr.LastApplied = -1
	nr.NextIndex = make([]int, 3)
	nr.MatchIndex = make([]int, 3)

	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)
		
		fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo + " -->> ",
								log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
			  kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.Logger = log.New(logOutputFile, 
						   logPrefix + " -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(io.Discard, "", 0)
	}

	// Añadir codigo de inicialización
	go raftStates(nr)
	

	return nr
}


// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
//
func (nr *NodoRaft) para() {
	go func() {time.Sleep(5 * time.Millisecond); os.Exit(0) } ()
}


func (nr *NodoRaft) duerme() {
	nr.Rol=""
	go func() {time.Sleep(50 * time.Millisecond);nr.Rol="leader"; os.Exit(0) } ()
	
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos 
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int = nr.CurrentTerm
	var esLider bool 
	var idLider int =nr.IdLider
	
	esLider = nr.Yo==nr.IdLider
	// Vuestro codigo aqui
	

	return yo, mandato, esLider, idLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
// 
// No hay garantia que esta operacion consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar 
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
															bool, int, string) {
	indice := -1
	mandato := -1
	EsLider := false
	idLider := -1
	valorADevolver := ""
	nr.Logger.Println("someter")

	// Vuestro codigo aqui
	if EsLider = nr.Yo==nr.IdLider; EsLider {
		indice = len(nr.Log)
		mandato = nr.CurrentTerm
		entry := Entry{indice, mandato, operacion}
		nr.Log = append(nr.Log, entry)
		idLider = nr.Yo
		nr.Mux.Unlock()
		valorADevolver = <-nr.Committed
	} else {
		nr.Mux.Unlock()
		idLider = nr.IdLider
	
	}
	return indice, mandato, EsLider, idLider, valorADevolver
}


// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr * NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}
func (nr * NodoRaft) DuermeNodo(args Vacio, reply *Vacio) error {
	defer nr.duerme()
	return nil
}

type EstadoParcial struct {
	Mandato	int
	EsLider bool
	IdLider	int
}

type EstadoRemoto struct {
	IdNodo	int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo,reply.Mandato,reply.EsLider,reply.IdLider = nr.obtenerEstado()
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
												reply *ResultadoRemoto) error {
	reply.IndiceRegistro,reply.Mandato, reply.EsLider,
			reply.IdLider,reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
type ArgsPeticionVoto struct {
	// Vuestros datos aqui
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
//
type RespuestaPeticionVoto struct {
	// Vuestros datos aqui
	Term int
	VoteGranted bool
}


// Metodo para RPC PedirVoto
//
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
										reply *RespuestaPeticionVoto) error {
	// Vuestro codigo aqui
	nr.Mux.Lock()
	if peticion.Term < nr.CurrentTerm{
		reply.Term = nr.CurrentTerm
		reply.VoteGranted = false
	}else if peticion.Term == nr.CurrentTerm &&
		peticion.CandidateId != nr.VotedFor {
			reply.Term = nr.CurrentTerm
			reply.VoteGranted = false
	}else if peticion.Term > nr.CurrentTerm {
		if(len(nr.Log) == 0 || peticion.LastLogTerm > nr.Log[len(nr.Log)-1].Mandato ||
			(peticion.LastLogTerm == nr.Log[len(nr.Log)-1].Mandato && peticion.LastLogIndex >= len(nr.Log)-1)){
			nr.CurrentTerm = peticion.Term
			nr.VotedFor = peticion.CandidateId
			reply.Term = nr.CurrentTerm
			reply.VoteGranted = true
		}else{
			nr.CurrentTerm = peticion.Term
			nr.VotedFor = nr.CurrentTerm
			reply.VoteGranted = false
		}

		if(nr.Rol == "candidate" || nr.Rol == "leader"){
			nr.FollowerChannel <- true
		}
	}
	nr.Mux.Unlock()
	return nil	
}


type ArgAppendEntries struct {
	// Vuestros datos aqui
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
	
}

type Results struct {
	// Vuestros datos aqui
	Term int
	Success bool
}


// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
													  results *Results) error {
	// Completar....
	nr.Mux.Lock()
	if(args.Term < nr.CurrentTerm){
		results.Term = nr.CurrentTerm
	}else if(args.Term == nr.CurrentTerm){
		nr.IdLider = args.LeaderId
		results.Term = nr.CurrentTerm
		if(len(nr.Log) == 0){
			if(len(args.Entries) != 0){
				nr.Log = append(nr.Log, args.Entries...)
				
			}
			results.Success = true
		}else if(args.PrevLogIndex > len(nr.Log)-1 || nr.Log[args.PrevLogIndex].Mandato != args.PrevLogTerm){
			results.Success = false
		}else{
			if(len(args.Entries) != 0){
				nr.Log = nr.Log[0 : args.PrevLogIndex+1]
				nr.Log = append(nr.Log, args.Entries...)
				
			}
			results.Success = true
		}
		if (args.LeaderCommit > nr.CommitIndex){
			nr.CommitIndex = min(args.LeaderCommit, len(nr.Log)-1)
		}
		nr.HearbeatChannel <- true
	}else{
		nr.IdLider = args.LeaderId
		nr.CurrentTerm = args.Term
		results.Term = nr.CurrentTerm
		if( nr.Rol == "leader"){
			nr.FollowerChannel <- true
		}else{
			if(args.LeaderCommit > nr.CommitIndex){
				nr.CommitIndex = min(args.LeaderCommit, len(nr.Log)-1)
			}
			nr.HearbeatChannel <- true
		}
	}
	nr.Mux.Unlock()

	return nil
}


// ----- Metodos/Funciones a utilizar como clientes
//
//

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petición perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre  todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
//
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
											reply *RespuestaPeticionVoto) bool {
	

	// Completar....

	error := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto", args, reply, 10*time.Millisecond)
	if(error != nil){
		return false
	}else{
		if(reply.Term > nr.CurrentTerm){
			nr.CurrentTerm = reply.Term
			nr.FollowerChannel <- true
		}else if(reply.VoteGranted){
			nr.MyVotes++
			if(nr.MyVotes > len(nr.Nodos)/2){
				nr.LeaderChannel <- true
			}
		}
		return true
	}

	
}


func (nr *NodoRaft) enviarHeartbeat(nodo int, args *ArgAppendEntries,
	result *Results) bool {


// Completar....

	error := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries", args, result, 10*time.Millisecond)
	if(error != nil){
		return false
	}else{
		if(result.Term > nr.CurrentTerm){
			nr.Mux.Lock()
			nr.CurrentTerm = result.Term
			nr.IdLider = -1
			nr.FollowerChannel <- true
			nr.Mux.Unlock()
		}
		return true
	}


}


func (nr *NodoRaft) nuevaEntrada(nodo int, args *ArgAppendEntries,
	result *Results) bool {

	error := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries", args, result, 10*time.Millisecond)
	if(error != nil){
		return false
	}else{
		if(result.Success){
			nr.MatchIndex[nodo] = nr.NextIndex[nodo]
			nr.NextIndex[nodo]++
			nr.Mux.Lock()
			if(nr.MatchIndex[nodo] > nr.CommitIndex){
				nr.Replies++
				if(nr.Replies == len(nr.Nodos)/2){
					nr.CommitIndex++
					nr.Replies=0
				}
			}
			nr.Mux.Unlock()
		}else{
			nr.NextIndex[nodo]--
		}
		return true
	}
}

func requestVotes(nr *NodoRaft){
	var reply RespuestaPeticionVoto
	for i:=0 ; i<len(nr.Nodos); i++{
		if(i!=nr.Yo){
			if(len(nr.Log)!=0 ){
				lastLogIndex :=len(nr.Log)-1
				lastLogTerm :=nr.Log[lastLogIndex].Mandato
				go nr.enviarPeticionVoto(i, &ArgsPeticionVoto{nr.CurrentTerm,nr.Yo, lastLogIndex, lastLogTerm}, &reply)
			}else{
				go nr.enviarPeticionVoto(i, &ArgsPeticionVoto{nr.CurrentTerm,nr.Yo, -1, 0}, &reply)
			}
		}
	}
}

func sendAppendEntries(nr *NodoRaft){
	var result Results
	for i:=0; i<len(nr.Nodos); i++{
		if(i != nr.Yo){
			if(len(nr.Log)-1 >= nr.NextIndex[i]){
				var entries []Entry 
				entries[0]= Entry{nr.NextIndex[i], nr.Log[nr.NextIndex[i]].Mandato, nr.Log[nr.NextIndex[i]].Operacion}
				if(nr.NextIndex[i]!=0){
					prevLogIndex := nr.NextIndex[i]-1
					prevLogTerm := nr.Log[prevLogIndex].Mandato
					go nr.nuevaEntrada(i, &ArgAppendEntries{nr.CurrentTerm,nr.Yo,prevLogIndex,prevLogTerm, entries,nr.CommitIndex},&result)
				}else{
					go nr.nuevaEntrada(i, &ArgAppendEntries{nr.CurrentTerm,nr.Yo,-1,0, entries,nr.CommitIndex},&result)

				}
			}else{
				var entries []Entry
				if(nr.NextIndex[i]!=0){
					prevLogIndex := nr.NextIndex[i]-1
					prevLogTerm := nr.Log[prevLogIndex].Mandato
					go nr.nuevaEntrada(i, &ArgAppendEntries{nr.CurrentTerm,nr.Yo,prevLogIndex,prevLogTerm, entries,nr.CommitIndex},&result)

				}else{
					go nr.nuevaEntrada(i, &ArgAppendEntries{nr.CurrentTerm,nr.Yo,-1,0, entries,nr.CommitIndex},&result)

				}
			}
		}
	
	}
}

func raftStates(nr *NodoRaft){
	for{
		nr.Logger.Println("EMPIEZAAA", nr.CommitIndex, nr.LastApplied)
		if(nr.CommitIndex > nr.LastApplied){
			nr.LastApplied++
			operacion := AplicaOperacion{nr.LastApplied, nr.Log[nr.LastApplied].Operacion}
			nr.OperacionChannel <- operacion
		}

		for nr.Rol == "follower"{
			nr.Logger.Println("soyFollower")
			select{
			case <-nr.HearbeatChannel:
				nr.Logger.Println("reciboHeartbeat")
				nr.Rol = "follower"
			case <-time.After(time.Duration(rand.Intn(101)+100) * time.Millisecond):
				nr.IdLider = -1
				nr.Rol = "candidate"
			}
		}

		for nr.Rol == "candidate"{
			nr.Logger.Println("soyCandidate", nr.CommitIndex, nr.LastApplied)
			if(nr.CommitIndex > nr.LastApplied){
				nr.LastApplied++
				operacion := AplicaOperacion{nr.LastApplied, nr.Log[nr.LastApplied].Operacion}
				nr.OperacionChannel <- operacion
			}

			nr.CurrentTerm++
			nr.VotedFor = nr.Yo
			nr.MyVotes = 1
			timer := time.NewTimer(time.Duration(rand.Intn(101)+100) * time.Millisecond)
			requestVotes(nr)
			select{
			case <-nr.HearbeatChannel:
				nr.Rol = "follower"
			case <- nr.FollowerChannel:
				nr.Rol = "follower"
			case <-timer.C:
				nr.Rol = "candidate"
			case <-nr.LeaderChannel:
				for i:=0; i<len(nr.Nodos); i++{
					if(i!=nr.Yo){
						nr.NextIndex[i] = len(nr.Log)
						nr.MatchIndex[i] = -1
					}
				}
				nr.Rol = "leader"
			}
		}
		for nr.Rol =="leader"{
			nr.Logger.Println("soyLeader")
			nr.IdLider = nr.Yo
			sendAppendEntries(nr)
			timer := time.NewTimer(50*time.Millisecond)
			select{
			case <-nr.FollowerChannel:
				nr.Rol = "follower"
			case <-timer.C:
				nr.Logger.Println("timer-leader", nr.CommitIndex, nr.LastApplied)
				if(nr.CommitIndex>nr.LastApplied){
					nr.LastApplied++
					//nr.Logger.Println("timer-leader2", nr.CommitIndex, nr.LastApplied)
					operacion := AplicaOperacion{nr.LastApplied, nr.Log[nr.LastApplied].Operacion}
					//nr.Logger.Println("timer-leader3", nr.CommitIndex, nr.LastApplied)
					nr.OperacionChannel <- operacion
					operacion = <- nr.OperacionChannel
					nr.Committed <- operacion.Operacion.Valor
					

				}
				nr.Rol = "leader"
			}
		}
	}
}

