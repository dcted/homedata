package main

import (
	"bufio"
	"os"
	"fmt"
	"log"
	"strings"
	"strconv"
)

type PropertyKey struct {
	id int
	valuationDate string
}

type PropertyEntry struct {
	key PropertyKey
	address string
	town string
	value string
}

var properties = make(map[PropertyKey]PropertyEntry)
var keys = []PropertyKey{}
//var properties = []PropertyEntry{}

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
				parseProperties(file, addAcceptFirstDuplicate)
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
		idint, err := strconv.Atoi(row[0])

		if(err != nil) {
			continue
		}

		propertyKey := PropertyKey{id: idint, valuationDate: row[3]}
		property := PropertyEntry{key: propertyKey, address: row[1], town: row[2], value: row[4]}
		filt(property)
	}
}

//----- Functions to run tests 1-4
func addAcceptLastDuplicate(prop PropertyEntry) {
	inserted := PropertiesInsert(prop)

	if !inserted {
		properties[prop.key] = prop
	}
}

func addAcceptFirstDuplicate(prop PropertyEntry) {
	//All we want to do is try and sorted insert the property
	//We dont care about the returned values because we dont want
	//to do anything extra for any outcome
	PropertiesInsert(prop)
}

func addAcceptNoDuplicate(prop PropertyEntry) {
	inserted := PropertiesInsert(prop)

	//If it was not inserted, remove the index returned also 
	//(index of the found matching value)
	if !inserted {
		delete(properties, prop.key)
	}
}

func addAfterFilter(prop PropertyEntry) {
	if CheckFilterUnder400k(prop) && CheckNoAveCresPlace(prop) && skipTenth() {
		PropertiesInsert(prop)
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

	go applyFilters(half, keys[0:len(keys)/2])
	go applyFilters(half, keys[len(keys)/2:len(keys)-1])

	merge(<-half, <-half)
}

func applyFilters(filtered chan []PropertyEntry, unfiltered []PropertyKey) {
	current := []PropertyEntry{}
	for _,val := range unfiltered {
		if value, exists := properties[val]; exists {
			if CheckFilterUnder400k(value) && CheckNoAveCresPlace(value) && skipTenth() {
				current = append(current, value)
			}
		}
	}
	filtered<-current
}

func merge(a []PropertyEntry, b []PropertyEntry) {
	merged := append(a,b...)
	printSliceResults(merged)
}

//----- Printer for printing all properties in a slice
func printResults(results map[PropertyKey]PropertyEntry) {
	for key,val := range results {
		fmt.Println(key.id, val.address, val.town, key.valuationDate, val.value)

	}
}

func printSliceResults(results []PropertyEntry) {
	for _,val := range results {
		fmt.Println(val.key.id, val.address, val.town, val.key.valuationDate, val.value)
	}
}

//Inserts into the properties map, using the props key property as the map key
//will return false if the value wasnt added because of a duplicate situation
//or true of it was successfully added with no duplicate
func PropertiesInsert(prop PropertyEntry) (bool) {
    if _, exists := properties[prop.key]; exists {
    	//We need to escalate this as there is a duplicate
    	return false
    } else {
    	//safe to just add as there is no duplicate
    	properties[prop.key] = prop
    	keys = append(keys, prop.key)
    	return true;
    }
}