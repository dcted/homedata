package main

import (
	"bufio"
	"os"
	"fmt"
	"log"
	"strings"
	"strconv"
)

type PropertyEntry struct {
	//make everything a string to make it easier to call INSERT INTO with the data
	id string
	address string
	town string
	valuationDate string
	value string
}

var properties = []PropertyEntry{}

//Keep this counter for skipping the 10th entry
var skipTenth = TenthCounter()

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Run this by including the data file path as the first agrument")
		fmt.Println("Also include a second argument of a value between 1 and 5 to run tests 1-5")
		return
	}

	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
		return
	}
	defer file.Close()

	value, error := strconv.Atoi(os.Args[2])
	if(error == nil) {
		if value < 1 || value > 5 {
			fmt.Println("Enter a 2nd argument (test number) between 1 and 5 only")
			return;
		}

		//Based on the 2nd arg, runs the test.
		//Tests 1 - 4 are tests 1 -4, and test 5 is test 4 extra credit.
		switch value {
			case 1:
				parseProperties(file, addAcceptLastDuplicate)
			case 2:
				parseProperties(file, addAcceptFirstDuplicate)
			case 3:
				parseProperties(file, addAcceptNoDuplicate)
			case 4:
				parseProperties(file, addAfterFilter)
			case 5:
				parseProperties(file, addAcceptLastDuplicate)
				SplitFilterMerge()
				return;
		}
		printResults(properties)
	} else {
		fmt.Println("Enter an integer value between 1 and 5 for second argument")
		return
	}
}

//----- Reads the properties file, creates PropertyEntry structs for data
//----- and runs testing and or filter functions on the PropertyEntrys
//----- which decide if or not to add them to the final slice
func parseProperties(file *os.File, filt func(PropertyEntry)) {
	scan := bufio.NewScanner(file)

	for scan.Scan() {
		row := strings.Split(scan.Text(), "\t")

		//if the row doesnt contain 5 data values as necessary, we'll disregard it
		if len(row) != 5 {
			continue
		}

		//If we cant parse the id into an int, assume the row is invalid/not a data row
		if _, err := strconv.Atoi(row[0]); err != nil {
			continue
		}

		property := PropertyEntry{id: row[0], address: row[1], town: row[2], valuationDate: row[3], value: row[4]}
		filt(property)
	}
}

//----- Functions to run tests 1-4
func addAcceptLastDuplicate(prop PropertyEntry) {
	index, exists := PropertiesContains(prop)

	//If it exists, replace it, else append it
	if exists {
		properties[index] = prop
	} else {
		properties = append(properties, prop)
	}
}

func addAcceptFirstDuplicate(prop PropertyEntry) {
	_, exists := PropertiesContains(prop)

	//if the property already exists, dont add it, add eveything else
	if !exists {
		properties = append(properties, prop)
	}
}

func addAcceptNoDuplicate(prop PropertyEntry) {
	index, exists := PropertiesContains(prop)

	//We'll add entries, but well remove entries if we find duplication
	//so that neither entry exists
	if exists {
		properties = append(properties[:index], properties[index+1:]...)
	} else {
		properties = append(properties, prop)
	}
}

func addAfterFilter(prop PropertyEntry) {
	if CheckFilterUnder400k(prop) && CheckNoAveCresPlace(prop) && skipTenth() {
		properties = append(properties, prop)
	}
}

//----- Filter functions. Each returns a bool of true if the property 
//----- passes the filter and ~should~ be added, or false if not
func CheckFilterUnder400k(prop PropertyEntry) bool {
	val, err := strconv.Atoi(prop.value)

	if err == nil && val > 400000 {
		return true
	}
	
	return false
}

func CheckNoAveCresPlace(prop PropertyEntry) bool {
	if !strings.HasSuffix(prop.address, "AVE") &&
		!strings.HasSuffix(prop.address, "CRES") &&
		 !strings.HasSuffix(prop.address, "PL") {
		return true
	}
	
	return false
}

//function closure to keep entry count (%10)
//Return a function returning bool to keep track
func TenthCounter() func() bool {
	count := 0

	return func() bool {
		count += 1

		if count >= 10 {
			count = 0
			return false
		}

		return true
	}
}

//----- Runs test 4 extra credit (test 5 in code) to split our preprocessed data
//----- and filter in it own go routines, merging the results
func SplitFilterMerge() {
	half := make(chan []PropertyEntry, 2)

	go applyFilters(half, properties[0:len(properties)/2])
	go applyFilters(half, properties[len(properties)/2:len(properties)-1])

	merge(<-half, <-half)
}

func applyFilters(filtered chan []PropertyEntry, unfiltered []PropertyEntry) {
	current := []PropertyEntry{}
	for _,val := range unfiltered {
		if CheckFilterUnder400k(val) && CheckNoAveCresPlace(val) && skipTenth() {
			current = append(current, val)
		}
	}
	filtered<-current
}

func merge(a []PropertyEntry, b []PropertyEntry) {
	merged := append(a,b...)
	printResults(merged)
}

//----- Printer for printing all properties in a slice
func printResults(results []PropertyEntry) {
	for _,val := range results {
		fmt.Println(val.id, val.address, val.town, val.valuationDate, val.value)
	}
}

//----- Helper methods to run equal to based on rules, and to check our property slice contains
//----- properties based on the equal to rules

//Method to check if two properties are equal (their address and valuationDates are the same)
func (p *PropertyEntry) EqualTo(prop PropertyEntry) bool {
	return strings.EqualFold(p.address, prop.address) && strings.EqualFold(p.valuationDate, prop.valuationDate)
}

//Function to check if the slice of PropertyEntry contains a given PropertyEntry dictated by the EqualTo method
func PropertiesContains(prop PropertyEntry) (int, bool) {
	for index, value := range properties {
		if value.EqualTo(prop) {
			return index, true
		}
	}
	return -1, false
}