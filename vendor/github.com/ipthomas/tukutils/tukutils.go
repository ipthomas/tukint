package tukutils

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

func PrettyTime(time string) string {
	return strings.Split(time, ".")[0]
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

func StringToInt(s string) int {
	if len(s) < 1 {
		return 0
	}
	i, e := strconv.Atoi(s)
	if e != nil {
		log.Println(e.Error())
	}
	return i
}
