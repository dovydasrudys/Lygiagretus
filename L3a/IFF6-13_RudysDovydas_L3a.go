// IFF-6/13
// Dovydas Rudys
package main

import (
	"fmt"
	"strings"
	"bufio"
	"os"
	"log"
	"strconv"
)

func main() {	//duomenų perdavimo - pagrindinis procesas
	list := ParallelList {List: make([]Request, 0)}
	readers, teams, players := ReadData("IFF6-13_RudysDovydas_L3a_dat_3.txt")
	writerChan := make(chan int)	//any2one rašytojų kanalas
	readerChan := make(chan int)	//any2one skaitytojų kanalas
	writerFinishChan := make(chan int, len(teams))	//kanalas nustatyti ar rašytojai baigė darbą
	readerFinishChan := make(chan int, len(readers))	//kanalas nustatyti ar skaitytojai baigė darbą
	finishChan := make(chan int)	//kanalas nustatyti kada visi procesai baigė darbus

	for i := 0; i < len(teams); i++{
		go Writer(teams, teams[i], writerChan, writerFinishChan)
	}

	for i := 0; i < len(readers); i++{
		go Reader(readers, readers[i], readerChan, readerFinishChan)
	}

	go Manager(&list.List, writerChan, readerChan, finishChan)
	<-finishChan
	WriteData("IFF6-13_RudysDovydas_L3a_rez.txt", players, readers)
	WriteResults("IFF6-13_RudysDovydas_L3a_rez.txt", list.List)
	//------------------------------
}
func Manager(list *[]Request, writerChan <-chan int, readerChan <-chan int, finishChan chan int){	// valdytojo procesas
	ageIn, writersOpen := <- writerChan
	ageOut, readersOpen := <- readerChan
	for{
		if writersOpen{	// jeigu rašytojų kanalas atidarytas

			Add(list, Request{Value: ageIn, Count: 1})	// įrašo naują žaidėją į sąrašą
			ageIn, writersOpen = <- writerChan	// nuskaito naują žaidėją iš kanalo

			if Contains(list, Request{Value: ageOut, Count: 1}){
				Remove(list, Request{Value: ageOut, Count: 1})
				ageOut = <- readerChan	// sekantis žaidėjas iš skaitytojų kanalo nuskaitomas tik tada, jeigu prieš jį buvęs jau apdorotas (ištrintas iš sąrašo)
			}
		}else{
			if readersOpen{	// jeigu rašytojų kanalas uždarytas (t.y rašytojai baigė savo darbą), bet yra likusių skaitytojų
				if Contains(list, Request{Value: ageOut, Count: 1}){
					Remove(list, Request{Value: ageOut, Count: 1})
				}
				ageOut, readersOpen = <- readerChan	// dabar jau nebesvarbu ar pavyko ištrinti ar ne, vistiek imamas sekantis žaidėjas iš skaitytojų kanalo
			}else{
				break
			}
		}
	}
	finishChan <- 1
}
func Writer(teams [][]Player, team []Player, writerChan chan<- int, writerFinishChan chan int){	// rašytojo procesas
	count := len(team)
	for i := 0; i < count; i++{
		writerChan <- team[i].Age
	}
	writerFinishChan <- 1	// baigęs darbą rašytojas papildo kanalą
	if len(writerFinishChan) == len(teams){// jeigu procesas buvo paskutinis kuris pildė kanalą, tai jį uždaro, taip leisdamas valdytojui žinoti kada visi rašytojai baigė darbus
		close(writerChan)
	}
}
func Reader(readers [][]Request, reader []Request, readerChan chan<- int, readerFinishChan chan int){ // skaitytojo procesas
	count := len(reader)
	for i := 0; i < count; i++{
		for j := 0; j < reader[i].Count; j++{
			readerChan <- reader[i].Value
		}
	}
	// lygiai taip pat kaip ir rašytojai, paskutinis skaitytojas uždaro kanalą
	readerFinishChan <- 1
	if len(readerFinishChan) == len(readers){
		close(readerChan)
	}
}
func WriteData(resultFile string, players []Player, readers [][]Request){	// pradinių duomenų spausdinimas į failą
	file, _ := os.Create(resultFile)
	writer := bufio.NewWriter(file)
	defer file.Close()
	bla := strings.Repeat("-",92)
	header := fmt.Sprintf("| %7s | %20s | %12s | %6s | %8s | %20s |\r\n%v\r\n","Eil. nr", "Vardas", "Pozicija", "Amžius", "Ūgis", "Klubas", bla)
	fmt.Fprintln(writer, header)
	nr := 1;
		for index := 0; index < len(players); index++ {
			eilute := fmt.Sprintf("| %7d | %20s | %12s | %6d | %8.2f | %20s |\r\n",nr, players[index].Name, players[index].Position, players[index].Age, players[index].Height, players[index].Club)
			fmt.Fprintln(writer, eilute)
			nr++
		}
	fmt.Fprintf(writer, "%v\r\n", bla)
	header = fmt.Sprintf("| Skaitytojo nr. | Vertė | Kiekis |\r\n");
	bla = strings.Repeat("-",35)
	fmt.Fprintln(writer, header)
	fmt.Fprintf(writer, "%v\r\n", bla)
	for i := 0; i < len(readers); i++ {
		for index := 0; index < len(readers[i]); index++ {
			eilute := fmt.Sprintf("| %14d | %5d | %6d |\r\n", i+1, readers[i][index].Value, readers[i][index].Count)
			fmt.Fprintln(writer, eilute)
		}
	}
	fmt.Fprintf(writer, "%v\r\n", bla)
	writer.Flush()
}
func WriteResults(resultFile string, list []Request){	// rezultatų spausdinimas į failą
	file, _ := os.OpenFile(resultFile, os.O_APPEND, 0666)
	writer := bufio.NewWriter(file)
	defer file.Close()
	header := fmt.Sprintf("|         Rezultatai       |\r\n| Eil nr. | Vertė | Kiekis |\r\n");
	bla := strings.Repeat("-",28)
	fmt.Fprintln(writer, header)
	fmt.Fprintf(writer, "%v\r\n", bla)
	for i := 0; i < len(list); i++ {
		eilute := fmt.Sprintf("| %7d | %5d | %6d |\r\n", i+1, list[i].Value, list[i].Count)
		fmt.Fprintln(writer, eilute)
	}
	fmt.Fprintf(writer, "%v\r\n", bla)
	writer.Flush()
}
func ReadData(dataFile string)([][]Request, [][]Player, []Player){	// duomenų nuskaitymas

	readers := make([][]Request, 0)
	tempReader := make([]Request, 0)
	teams := make([][]Player, 0)
	tempTeam := make([]Player, 0)
	players := make([]Player, 0)

	file, err := os.Open(dataFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		attributes := strings.Split(scanner.Text(), ";")
		if len(attributes) != 5{
			if len(attributes) == 1{
				readers = append(readers, tempReader)
				tempReader = make([]Request, 0)
				continue
			}
			if len(attributes) == 2{
				value, _ := strconv.Atoi(attributes[0])
				count, _ := strconv.Atoi(attributes[1])
				tempRequest := Request{
					Value: value,
					Count: count,
				}
				tempReader = append(tempReader,tempRequest)
				continue
			}
		}else{
			name := attributes[0]
			position := attributes[1]
			age, _ := strconv.Atoi(attributes[2])
			height, _ := strconv.ParseFloat(attributes[3], 64)
			club := attributes[4]
			tempPlayer := Player{
				Name: name,
				Position: position,
				Age: age,
				Height: height,
				Club: club,
			}
			players = append(players, tempPlayer)
			teamIndex := len(teams)
			for i := 0; i < len(teams); i++{
				if teams[i][0].Club == tempPlayer.Club{
					teamIndex = i
				}
			}
			if teamIndex < len(teams){
				teams[teamIndex] = append(teams[teamIndex],tempPlayer)
			}else{
				tempTeam = make([]Player, 0)
				tempTeam = append(tempTeam, tempPlayer)
				teams = append(teams, tempTeam)
			}
		}
    	//fmt.Println(scanner.Text())
	}
	readers = append(readers, tempReader)
	return readers, teams, players
}
//Žaidėjo klasė
type Player struct {  
    Name string
    Position string
    Age int
	Height float64
	Club string
}
//
//Pageidavimo klasė
type Request struct {
	Value int
	Count int
}
//
//Duomenų struktūra
type ParallelList struct {
	List []Request
}
func Contains(list *[]Request, req Request) bool{
	for i := 0; i < len(*list); i++{
		if (*list)[i].Value == req.Value{
			return true;
		}
	}
	return false;
}
func Add(list *[]Request, req Request){
	index := len(*list)-1
	if index == -1{	// jeigu sąrašas tuščias
		*list = append(*list, req)
	}else{
		index = 0
		rado := false
		for i := 0; i < len(*list); i++{	// ieško indekso elemento, kuris lygus arba mažesnis už pridedamą
			if (*list)[i].Value <= req.Value{
				index = i
				rado = true
			}
		}
		if !rado{	// jeigu nerado, vadinasi pridėti reikia į pradžią, tam kad išlaikyti rikiuotą sąrašą
			tempRequest := Request{
				Value: 0,
				Count: 0,
			}
			*list = append(*list, tempRequest)
			copy((*list)[1:], (*list)[0:])
			(*list)[0] = req
			return
		}else{	// jeigu rado
			if (*list)[index].Value == req.Value{ // jeigu rado, ir tai yra toks pat elementas, tai padidinamas jo kiekis
				(*list)[index].Count++
			}else{	// jeigu rado, bet tai nėra toks pat elementas, vadinasi jis yra mažesnis už norimą pridėti, taigi naujas elementas pridedamas už rasto indekso
				index++
				tempRequest := Request{
					Value: 0,
					Count: 0,
				}
				*list = append(*list, tempRequest)
				copy((*list)[index+1:], (*list)[index:])
				(*list)[index] = req
			}
		}
	}
}
func Remove(list *[]Request, req Request){
	index := -1
	for i := 0; i < len(*list); i++{
		if (*list)[i].Value == req.Value{
			index = i
		}
	}
	if (*list)[index].Count > 1{	// jei trinamo elemento kiekis didesnis už 1 tai jis mažinamas
		(*list)[index].Count--
	}else{
		if (*list)[index].Count == 1{	// kitu atveju pašalinamas pats elementas
			(*list) = append((*list)[:index], (*list)[index+1:]...)
		}
	}
}