package tukutil

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	SeedRoot   = "1.2.40.0.13.1.1.3542466645."
	IdSeed     = GetIdIncrementSeed(5)
	CodeSystem = make(map[string]string)
)

func SetCodeSystem(cs map[string]string) {
	CodeSystem = cs
}

func InitCodeSystem(basepath string, configFolder string, codesystemFile string) error {
	file, err := os.Open(basepath + "/" + configFolder + "/" + codesystemFile)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	if err = json.NewDecoder(file).Decode(&CodeSystem); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("Loaded %v code system key values", len(CodeSystem))
	return nil
}
func GetCodeSystemVal(key string) string {
	val, ok := CodeSystem[key]
	if ok {
		return val
	}
	return key
}

// returns unique id in format '1.2.40.0.13.1.1.3542466645.20211021090059143.32643'
// idroot constant - 1.2.40.0.13.1.1.3542466645.
// + datetime	   - 20211021090059143.
// + 5 digit seed  - 32643
// The seed is incremented after each call to newid().
func Newid() string {
	id := SeedRoot + DT_yyyyMMddhhmmSSsss() + "." + GetStringFromInt(IdSeed)
	IdSeed = IdSeed + 1
	return id
}

// Tuk_Time returns current time for location Europe/London
func Tuk_Time() string {
	location, err := time.LoadLocation("Europe/London")
	if err != nil {
		log.Println(err.Error())
		return time.Now().String()
	}
	return time.Now().In(location).String()
}
func GetMilliseconds() int {
	return GetIntFromString(Substr(GetStringFromInt(time.Now().Nanosecond()), 0, 3))
}
func PrettyTime(time string) string {
	return strings.Split(time, ".")[0]
}
func Tuk_Hour() string {
	return fmt.Sprintf("%02d",
		time.Now().Local().Hour())
}
func Tuk_Min() string {
	return fmt.Sprintf("%02d", time.Now().Local().Minute())
}
func Tuk_Sec() string {
	return fmt.Sprintf("%02d",
		time.Now().Local().Second())
}
func Tuk_MilliSec() int {
	return GetMilliseconds()
}
func Tuk_Day() string {
	return fmt.Sprintf("%02d",
		time.Now().Local().Day())
}
func Tuk_Year() string {
	return fmt.Sprintf("%d",
		time.Now().Local().Year())
}
func Tuk_Month() string {
	return fmt.Sprintf("%02d",
		time.Now().Local().Month())
}
func NewUuid() string {
	u := uuid.New()
	return u.String()
}
func GetIdIncrementSeed(len int) int {
	return GetIntFromString(Substr(GetStringFromInt(time.Now().Nanosecond()), 0, len))
}
func DT_yyyyMMddhhmmSSsss() string {
	return Tuk_Year() + Tuk_Month() + Tuk_Day() + Tuk_Hour() + Tuk_Min() + Tuk_Sec() + strconv.Itoa(Tuk_MilliSec())
}
func GetStringFromInt(i int) string {
	return strconv.Itoa(i)
}
func GetIntFromString(s string) int {
	i, e := strconv.Atoi(s)
	if e != nil {
		log.Println(e.Error())
	}
	return i
}
func Substr(input string, start int, length int) string {
	asRunes := []rune(input)
	if start >= len(asRunes) {
		return ""
	}
	if start+length > len(asRunes) {
		length = len(asRunes) - start
	}
	return string(asRunes[start : start+length])
}
func GetXMLNodeList(message string, node string) string {
	if strings.Contains(message, node) {
		var nodeopen = "<" + node
		var nodeclose = "</" + node + ">"
		log.Println("Searching for XML Element: " + nodeopen + ">")
		var start = strings.Index(message, nodeopen)
		var end = strings.Index(message, nodeclose) + len(nodeclose)
		m := message[start:end]
		log.Println("Extracted XML Element Nodelist")
		return m
	}
	log.Println("Message does not contain Element : " + node)
	return ""
}
