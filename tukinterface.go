package tukint

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	cnst "github.com/ipthomas/tukcnst"
	util "github.com/ipthomas/tukutil"
)

type TUKServiceState struct {
	LogEnabled          bool   `json:"logenabled"`
	Paused              bool   `json:"paused"`
	Scheme              string `json:"scheme"`
	Host                string `json:"host"`
	Port                int    `json:"port"`
	Url                 string `json:"url"`
	User                string `json:"user"`
	Password            string `json:"password"`
	Org                 string `json:"org"`
	Role                string `json:"role"`
	POU                 string `json:"pou"`
	ClaimDialect        string `json:"claimdialect"`
	ClaimValue          string `json:"claimvalue"`
	BaseFolder          string `json:"basefolder"`
	LogFolder           string `json:"logfolder"`
	ConfigFolder        string `json:"configfolder"`
	TemplatesFolder     string `json:"templatesfolder"`
	Secret              string `json:"secret"`
	Token               string `json:"token"`
	CertPath            string `json:"certpath"`
	Certs               string `json:"certs"`
	Keys                string `json:"keys"`
	DBSrvc              string `json:"dbsrvc"`
	STSSrvc             string `json:"stssrvc"`
	SAMLSrvc            string `json:"samlsrvc"`
	LoginSrvc           string `json:"loginsrvc"`
	PIXSrvc             string `json:"pixsrvc"`
	CacheTimeout        int    `json:"cachetimeout"`
	CacheEnabled        bool   `json:"cacheenabled"`
	ContextTimeout      int    `json:"contexttimeout"`
	TUK_DB_URL          string `json:"tukdburl"`
	DSUB_Broker_URL     string `json:"dsubbrokerurl"`
	DSUB_Consumer_URL   string `json:"dsubconsumerurl"`
	DSUB_Subscriber_URL string `json:"dsubsubscriberurl"`
	PIXm_URL            string `json:"pixmurl"`
	XDS_Reg_URL         string `json:"xdsregurl"`
	XDS_Rep_URL         string `json:"xdsrepurl"`
	NHS_OID             string `json:"nhsoid"`
	Regional_OID        string `json:"regionaloid"`
}
type Dashboard struct {
	Total      int
	Open       int
	InProgress int
	Closed     int
}
type TmpltWorkflow struct {
	Created   string
	NHS       string
	Pathway   string
	XDWKey    string
	Published bool
	Version   int
	XDW       XDWWorkflowDocument
}
type TmpltWorkflows struct {
	Count     int
	Workflows []TmpltWorkflow
}
type WorkflowState struct {
	Events    Events    `json:"events"`
	XDWS      TUKXDWS   `json:"xdws"`
	Workflows Workflows `json:"workflows"`
}
type TUKXDWS struct {
	Action       string   `json:"action"`
	LastInsertId int64    `json:"lastinsertid"`
	Count        int      `json:"count"`
	XDW          []TUKXDW `json:"xdws"`
}
type TUKXDW struct {
	Id        int    `json:"id"`
	Name      string `json:"name"`
	IsXDSMeta bool   `json:"isxdsmeta"`
	XDW       string `json:"xdw"`
}
type DSUBSubscribeResponse struct {
	XMLName        xml.Name `xml:"Envelope"`
	Text           string   `xml:",chardata"`
	S              string   `xml:"s,attr"`
	A              string   `xml:"a,attr"`
	Xsi            string   `xml:"xsi,attr"`
	Wsnt           string   `xml:"wsnt,attr"`
	SchemaLocation string   `xml:"schemaLocation,attr"`
	Header         struct {
		Text   string `xml:",chardata"`
		Action string `xml:"Action"`
	} `xml:"Header"`
	Body struct {
		Text              string `xml:",chardata"`
		SubscribeResponse struct {
			Text                  string `xml:",chardata"`
			SubscriptionReference struct {
				Text    string `xml:",chardata"`
				Address string `xml:"Address"`
			} `xml:"SubscriptionReference"`
		} `xml:"SubscribeResponse"`
	} `xml:"Body"`
}
type DSUBSubscribe struct {
	BrokerUrl   string
	ConsumerUrl string
	Topic       string
	Expression  string
	Request     []byte
	BrokerRef   string
}
type DSUBAcknowledgement struct {
	Acknowledgement []byte
}
type DSUBCancel struct {
	BrokerRef string
	UUID      string
	Request   []byte
}
type DSUBNotifyMessage struct {
	XMLName             xml.Name `xml:"Notify"`
	Text                string   `xml:",chardata"`
	Xmlns               string   `xml:"xmlns,attr"`
	Xsd                 string   `xml:"xsd,attr"`
	Xsi                 string   `xml:"xsi,attr"`
	NotificationMessage struct {
		Text                  string `xml:",chardata"`
		SubscriptionReference struct {
			Text    string `xml:",chardata"`
			Address struct {
				Text  string `xml:",chardata"`
				Xmlns string `xml:"xmlns,attr"`
			} `xml:"Address"`
		} `xml:"SubscriptionReference"`
		Topic struct {
			Text    string `xml:",chardata"`
			Dialect string `xml:"Dialect,attr"`
		} `xml:"Topic"`
		ProducerReference struct {
			Text    string `xml:",chardata"`
			Address struct {
				Text  string `xml:",chardata"`
				Xmlns string `xml:"xmlns,attr"`
			} `xml:"Address"`
		} `xml:"ProducerReference"`
		Message struct {
			Text                 string `xml:",chardata"`
			SubmitObjectsRequest struct {
				Text               string `xml:",chardata"`
				Lcm                string `xml:"lcm,attr"`
				RegistryObjectList struct {
					Text            string `xml:",chardata"`
					Rim             string `xml:"rim,attr"`
					ExtrinsicObject struct {
						Text       string `xml:",chardata"`
						A          string `xml:"a,attr"`
						ID         string `xml:"id,attr"`
						MimeType   string `xml:"mimeType,attr"`
						ObjectType string `xml:"objectType,attr"`
						Slot       []struct {
							Text      string `xml:",chardata"`
							Name      string `xml:"name,attr"`
							ValueList struct {
								Text  string   `xml:",chardata"`
								Value []string `xml:"Value"`
							} `xml:"ValueList"`
						} `xml:"Slot"`
						Name struct {
							Text            string `xml:",chardata"`
							LocalizedString struct {
								Text  string `xml:",chardata"`
								Value string `xml:"value,attr"`
							} `xml:"LocalizedString"`
						} `xml:"Name"`
						Description    string `xml:"Description"`
						Classification []struct {
							Text                 string `xml:",chardata"`
							ClassificationScheme string `xml:"classificationScheme,attr"`
							ClassifiedObject     string `xml:"classifiedObject,attr"`
							ID                   string `xml:"id,attr"`
							NodeRepresentation   string `xml:"nodeRepresentation,attr"`
							ObjectType           string `xml:"objectType,attr"`
							Slot                 []struct {
								Text      string `xml:",chardata"`
								Name      string `xml:"name,attr"`
								ValueList struct {
									Text  string   `xml:",chardata"`
									Value []string `xml:"Value"`
								} `xml:"ValueList"`
							} `xml:"Slot"`
							Name struct {
								Text            string `xml:",chardata"`
								LocalizedString struct {
									Text  string `xml:",chardata"`
									Value string `xml:"value,attr"`
								} `xml:"LocalizedString"`
							} `xml:"Name"`
						} `xml:"Classification"`
						ExternalIdentifier []struct {
							Text                 string `xml:",chardata"`
							ID                   string `xml:"id,attr"`
							IdentificationScheme string `xml:"identificationScheme,attr"`
							ObjectType           string `xml:"objectType,attr"`
							RegistryObject       string `xml:"registryObject,attr"`
							Value                string `xml:"value,attr"`
							Name                 struct {
								Text            string `xml:",chardata"`
								LocalizedString struct {
									Text  string `xml:",chardata"`
									Value string `xml:"value,attr"`
								} `xml:"LocalizedString"`
							} `xml:"Name"`
						} `xml:"ExternalIdentifier"`
					} `xml:"ExtrinsicObject"`
				} `xml:"RegistryObjectList"`
			} `xml:"SubmitObjectsRequest"`
		} `xml:"Message"`
	} `xml:"NotificationMessage"`
}
type PIXmResponse struct {
	ResourceType string `json:"resourceType"`
	ID           string `json:"id"`
	Type         string `json:"type"`
	Total        int    `json:"total"`
	Link         []struct {
		Relation string `json:"relation"`
		URL      string `json:"url"`
	} `json:"link"`
	Entry []struct {
		FullURL  string `json:"fullUrl"`
		Resource struct {
			ResourceType string `json:"resourceType"`
			ID           string `json:"id"`
			Identifier   []struct {
				Use    string `json:"use,omitempty"`
				System string `json:"system"`
				Value  string `json:"value"`
			} `json:"identifier"`
			Active bool `json:"active"`
			Name   []struct {
				Use    string   `json:"use"`
				Family string   `json:"family"`
				Given  []string `json:"given"`
			} `json:"name"`
			Gender    string `json:"gender"`
			BirthDate string `json:"birthDate"`
			Address   []struct {
				Use        string   `json:"use"`
				Line       []string `json:"line"`
				City       string   `json:"city"`
				PostalCode string   `json:"postalCode"`
				Country    string   `json:"country"`
			} `json:"address"`
		} `json:"resource"`
	} `json:"entry"`
}
type PIXmQuery struct {
	Count    int          `json:"count"`
	PIDOID   string       `json:"pidoid"`
	PID      string       `json:"pid"`
	REGOID   string       `json:"regoid"`
	REGID    string       `json:"regid"`
	NHSOID   string       `json:"nhsoid"`
	NHSID    string       `json:"nhsid"`
	Response []PIXPatient `json:"response"`
}
type PIXPatient struct {
	PIDOID     string `json:"pidoid"`
	PID        string `json:"pid"`
	REGOID     string `json:"regoid"`
	REGID      string `json:"regid"`
	NHSOID     string `json:"nhsoid"`
	NHSID      string `json:"nhsid"`
	GivenName  string `json:"givenname"`
	FamilyName string `json:"familyname"`
	Gender     string `json:"gender"`
	BirthDate  string `json:"birthdate"`
	Street     string `json:"street"`
	Town       string `json:"town"`
	City       string `json:"city"`
	State      string `json:"state"`
	Country    string `json:"country"`
	Zip        string `json:"zip"`
}
type XDWS struct {
	Action       string `json:"action"`
	LastInsertId int64  `json:"lastinsertid"`
	Count        int    `json:"count"`
	XDW          []XDW  `json:"xdws"`
}
type XDW struct {
	Id        int    `json:"id"`
	Name      string `json:"name"`
	IsXDSMeta bool   `json:"isxdsmeta"`
	XDW       string `json:"xdw"`
}
type IDMaps struct {
	Action       string  `json:"action"`
	LastInsertId int64   `json:"lastinsertid"`
	Count        int     `json:"count"`
	IDMaps       []IdMap `json:"idmaps"`
}
type IdMap struct {
	Id  int    `json:"id"`
	Lid string `json:"lid"`
	Mid string `json:"mid"`
}
type Workflow struct {
	Id        int    `json:"id"`
	Created   string `json:"created"`
	XDW_Key   string `json:"xdw_key"`
	XDW_UID   string `json:"xdw_uid"`
	XDW_Doc   string `json:"xdw_doc"`
	XDW_Def   string `json:"xdw_def"`
	Version   int    `json:"version"`
	Published bool   `json:"published"`
}
type Workflows struct {
	Action       string     `json:"action"`
	LastInsertId int64      `json:"lastinsertid"`
	Count        int        `json:"count"`
	Workflows    []Workflow `json:"workflows"`
}
type Subscription struct {
	Id         int    `json:"id"`
	Created    string `json:"created"`
	BrokerRef  string `json:"brokerref"`
	Pathway    string `json:"pathway"`
	Topic      string `json:"topic"`
	Expression string `json:"expression"`
}
type Subscriptions struct {
	Action        string         `json:"action"`
	LastInsertId  int64          `json:"lastinsertid"`
	Count         int            `json:"count"`
	Subscriptions []Subscription `json:"Subscriptions"`
}
type Event struct {
	EventId             int64               `json:"eventid"`
	Creationtime        string              `json:"creationtime"`
	DocName             string              `json:"docname"`
	ClassCode           string              `json:"classcode"`
	ConfCode            string              `json:"confcode"`
	FormatCode          string              `json:"formatcode"`
	FacilityCode        string              `json:"facilitycode"`
	PracticeCode        string              `json:"practicecode"`
	Expression          string              `json:"expression"`
	Authors             string              `json:"authors"`
	XdsPid              string              `json:"xdspid"`
	XdsDocEntryUid      string              `json:"xdsdocentryuid"`
	RepositoryUniqueId  string              `json:"repositoryuniqueid"`
	NhsId               string              `json:"nhsid"`
	User                string              `json:"user"`
	Org                 string              `json:"org"`
	Role                string              `json:"role"`
	Topic               string              `json:"topic"`
	Pathway             string              `json:"pathway"`
	Notes               string              `json:"notes"`
	Version             string              `json:"ver"`
	BrokerRef           string              `json:"brokerref"`
	XDWWorkflowDocument XDWWorkflowDocument `json:"xdwworkflowdocument"`
	Events              Events              `json:"events"`
}
type Events struct {
	Action       string  `json:"action"`
	LastInsertId int64   `json:"lastinsertid"`
	Count        int     `json:"count"`
	Events       []Event `json:"events"`
}
type XDWWorkflowDocument struct {
	XMLName                        xml.Name              `xml:"XDW.WorkflowDocument"`
	Hl7                            string                `xml:"hl7,attr"`
	WsHt                           string                `xml:"ws-ht,attr"`
	Xdw                            string                `xml:"xdw,attr"`
	Xsi                            string                `xml:"xsi,attr"`
	SchemaLocation                 string                `xml:"schemaLocation,attr"`
	ID                             ID                    `xml:"id"`
	EffectiveTime                  EffectiveTime         `xml:"effectiveTime"`
	ConfidentialityCode            ConfidentialityCode   `xml:"confidentialityCode"`
	Patient                        PatientID             `xml:"patient"`
	Author                         Author                `xml:"author"`
	WorkflowInstanceId             string                `xml:"workflowInstanceId"`
	WorkflowDocumentSequenceNumber string                `xml:"workflowDocumentSequenceNumber"`
	WorkflowStatus                 string                `xml:"workflowStatus"`
	WorkflowStatusHistory          WorkflowStatusHistory `xml:"workflowStatusHistory"`
	WorkflowDefinitionReference    string                `xml:"workflowDefinitionReference"`
	TaskList                       TaskList              `xml:"TaskList"`
}
type WorkflowDefinition struct {
	Ref                 string `json:"ref"`
	Name                string `json:"name"`
	Confidentialitycode string `json:"confidentialitycode"`
	CompleteByTime      string `json:"completebytime"`
	CompletionBehavior  []struct {
		Completion struct {
			Condition string `json:"condition"`
		} `json:"completion"`
	} `json:"completionBehavior"`
	Tasks []struct {
		ID                 string `json:"id"`
		Tasktype           string `json:"tasktype"`
		Name               string `json:"name"`
		Description        string `json:"description"`
		Owner              string `json:"owner"`
		ExpirationTime     string `json:"expirationtime"`
		StartByTime        string `json:"startbytime"`
		CompleteByTime     string `json:"completebytime"`
		IsSkipable         bool   `json:"isskipable"`
		CompletionBehavior []struct {
			Completion struct {
				Condition string `json:"condition"`
			} `json:"completion"`
		} `json:"completionBehavior"`
		Input []struct {
			Name        string `json:"name"`
			Contenttype string `json:"contenttype"`
			AccessType  string `json:"accesstype"`
		} `json:"input,omitempty"`
		Output []struct {
			Name        string `json:"name"`
			Contenttype string `json:"contenttype"`
			AccessType  string `json:"accesstype"`
		} `json:"output,omitempty"`
	} `json:"tasks"`
}
type ConfidentialityCode struct {
	Code string `xml:"code,attr"`
}
type EffectiveTime struct {
	Value string `xml:"value,attr"`
}
type PatientID struct {
	ID ID `xml:"id"`
}
type Author struct {
	AssignedAuthor AssignedAuthor `xml:"assignedAuthor"`
}
type AssignedAuthor struct {
	ID             ID             `xml:"id"`
	AssignedPerson AssignedPerson `xml:"assignedPerson"`
}
type ID struct {
	Root                   string `xml:"root,attr"`
	Extension              string `xml:"extension,attr"`
	AssigningAuthorityName string `xml:"assigningAuthorityName,attr"`
}
type AssignedPerson struct {
	Name Name `xml:"name"`
}
type Name struct {
	Family string `xml:"family"`
	Prefix string `xml:"prefix"`
}
type WorkflowStatusHistory struct {
	DocumentEvent []DocumentEvent `xml:"documentEvent"`
}
type TaskList struct {
	XDWTask []XDWTask `xml:"XDWTask"`
}
type XDWTask struct {
	TaskData         TaskData         `xml:"taskData"`
	TaskEventHistory TaskEventHistory `xml:"taskEventHistory"`
}
type TaskData struct {
	TaskDetails TaskDetails `xml:"taskDetails"`
	Description string      `xml:"description"`
	Input       []Input     `xml:"input"`
	Output      []Output    `xml:"output"`
}
type TaskDetails struct {
	ID                    string `xml:"id"`
	TaskType              string `xml:"taskType"`
	Name                  string `xml:"name"`
	Status                string `xml:"status"`
	ActualOwner           string `xml:"actualOwner"`
	CreatedTime           string `xml:"createdTime"`
	CreatedBy             string `xml:"createdBy"`
	LastModifiedTime      string `xml:"lastModifiedTime"`
	RenderingMethodExists string `xml:"renderingMethodExists"`
}
type TaskEventHistory struct {
	TaskEvent []TaskEvent `xml:"taskEvent"`
}
type AttachmentInfo struct {
	Identifier      string `xml:"identifier"`
	Name            string `xml:"name"`
	AccessType      string `xml:"accessType"`
	ContentType     string `xml:"contentType"`
	ContentCategory string `xml:"contentCategory"`
	AttachedTime    string `xml:"attachedTime"`
	AttachedBy      string `xml:"attachedBy"`
	HomeCommunityId string `xml:"homeCommunityId"`
}
type Part struct {
	Name           string         `xml:"name,attr"`
	AttachmentInfo AttachmentInfo `xml:"attachmentInfo"`
}
type Output struct {
	Part Part `xml:"part"`
}
type Input struct {
	Part Part `xml:"part"`
}
type DocumentEvent struct {
	EventTime           string `xml:"eventTime"`
	EventType           string `xml:"eventType"`
	TaskEventIdentifier string `xml:"taskEventIdentifier"`
	Author              string `xml:"author"`
	PreviousStatus      string `xml:"previousStatus"`
	ActualStatus        string `xml:"actualStatus"`
}
type TaskEvent struct {
	ID         string `xml:"id"`
	EventTime  string `xml:"eventTime"`
	Identifier string `xml:"identifier"`
	EventType  string `xml:"eventType"`
	Status     string `xml:"status"`
}
type ClientRequest struct {
	Request      *http.Request
	Act          string `json:"act"`
	User         string `json:"user"`
	Org          string `json:"org"`
	Orgoid       string `json:"orgoid"`
	Role         string `json:"role"`
	NHS          string `json:"nhs"`
	PID          string `json:"pid"`
	PIDOrg       string `json:"pidorg"`
	PIDOID       string `json:"pidoid"`
	FamilyName   string `json:"familyname"`
	GivenName    string `json:"givenname"`
	DOB          string `json:"dob"`
	Gender       string `json:"gender"`
	ZIP          string `json:"zip"`
	Status       string `json:"status"`
	XDWKey       string `json:"xdwkey"`
	ID           int    `json:"id"`
	Task         string `json:"task"`
	Pathway      string `json:"pathway"`
	Version      int    `json:"version"`
	ReturnFormat string `json:"returnformat"`
}
type EventMessage struct {
	Message string
}

var (
	BaseFolder      = ""
	ConfigFolder    = "configs"
	CodeSystemFile  = "codesystem.json"
	TUK_DB_URL      = "https://5k2o64mwt5.execute-api.eu-west-1.amazonaws.com/beta/"
	DSUB_BROKER_URL = "http://spirit-test-01.tianispirit.co.uk:8081/SpiritXDSDsub/Dsub"
	PIX_MANAGER_URL = "http://spirit-test-01.tianispirit.co.uk:8081/SpiritPIXFhir/r4/Patient"
	REGIONAL_OID    = "2.16.840.1.113883.2.1.3.31.2.1.1"
	NHS_OID         = "2.16.840.1.113883.2.1.4.1"
)

func SetNHSOID(nhsoid string) {
	NHS_OID = nhsoid
}
func SetRegionalOID(regionaloid string) {
	REGIONAL_OID = regionaloid
}
func SetBaseFolder(baseFolder string) {
	BaseFolder = baseFolder
}
func SetConfigFolder(configFolder string) {
	ConfigFolder = configFolder
}
func SetCodeSystemFile(csfile string) {
	CodeSystemFile = csfile
	if BaseFolder != "" {
		InitCodeSystem()
	}
}
func InitCodeSystem() {
	util.InitCodeSystem(BaseFolder, ConfigFolder, CodeSystemFile)
}
func InitLambdaVars() {
	if os.Getenv("TUK_DB_URL") != "" {
		TUK_DB_URL = os.Getenv("TUK_DB_URL")
		log.Printf("Set TUK_DB_URL environment variable - %s", TUK_DB_URL)
	} else {
		log.Println("Unable to set TUK_DB_URL environment variable. No Value found!")
	}
	if os.Getenv("PIX_MANAGER_URL") != "" {
		PIX_MANAGER_URL = os.Getenv("PIX_MANAGER_URL")
		log.Printf("Set PIX_MANAGER_URL environment variable - %s", PIX_MANAGER_URL)
	} else {
		log.Println("Unable to set PIX_MANAGER_URL environment variable. No Value found!")
	}
	if os.Getenv("PIX_MANAGER_URL") != "" {
		DSUB_BROKER_URL = os.Getenv("DSUB_BROKER_URL")
		log.Printf("Set DSUB_BROKER_URL environment variable - %s", DSUB_BROKER_URL)
	} else {
		log.Println("Unable to set DSUB_BROKER_URL environment variable. No Value found!")
	}
	if os.Getenv("REGIONAL_OID") != "" {
		REGIONAL_OID = os.Getenv("REGIONAL_OID")
		log.Printf("Set REGIONAL_OID environment variable - %s", REGIONAL_OID)
	} else {
		log.Println("Unable to set REGIONAL_OID environment variable. No Value found!")
	}
	if os.Getenv("NHS_OID") != "" {
		NHS_OID = os.Getenv("NHS_OID")
		log.Printf("Set NHS_OID environment variable - %s", NHS_OID)
	} else {
		log.Println("Unable to set NHS_OID environment variable. No Value found!")
	}
}
func (i *ClientRequest) InitClientRequest() error {
	if i.Request == nil {
		return errors.New("clientrequest.request is not set")
	}
	log.Printf("Received http %s request", i.Request.Method)
	i.Request.ParseForm()
	i.Act = i.Request.FormValue("act")
	i.User = i.Request.FormValue("user")
	i.Org = i.Request.FormValue("org")
	i.Orgoid = util.GetCodeSystemVal(i.Request.FormValue("org"))
	i.Role = i.Request.FormValue("role")
	i.NHS = i.Request.FormValue("nhs")
	i.PID = i.Request.FormValue("pid")
	i.PIDOrg = i.Request.FormValue("pidorg")
	i.PIDOID = util.GetCodeSystemVal(i.Request.FormValue("pidorg"))
	i.FamilyName = i.Request.FormValue("familyname")
	i.GivenName = i.Request.FormValue("givenname")
	i.DOB = i.Request.FormValue("dob")
	i.Gender = i.Request.FormValue("gender")
	i.ZIP = i.Request.FormValue("zip")
	i.Status = i.Request.FormValue("status")
	i.ID = util.GetIntFromString(i.Request.FormValue("id"))
	i.Task = i.Request.FormValue("task")
	i.Pathway = i.Request.FormValue("pathway")
	i.Version = util.GetIntFromString(i.Request.FormValue("version"))
	i.XDWKey = i.Request.FormValue("xdwkey")
	i.ReturnFormat = i.Request.Header.Get(cnst.ACCEPT)
	res2B, _ := json.MarshalIndent(i, "", "  ")
	log.Printf("Client Request\n%+v", string(res2B))
	return nil
}
func (i *EventMessage) NewEvent() error {
	dsubNotify, err := i.initDSUBNotifyMessage()
	if err != nil {
		return err
	}
	slots := dsubNotify.NotificationMessage.Message.SubmitObjectsRequest.RegistryObjectList.ExtrinsicObject
	tukevent := Event{
		EventId:             0,
		Creationtime:        util.Tuk_Time(),
		DocName:             slots.Name.LocalizedString.Value,
		ClassCode:           cnst.NO_VALUE,
		ConfCode:            cnst.NO_VALUE,
		FormatCode:          cnst.NO_VALUE,
		FacilityCode:        cnst.NO_VALUE,
		PracticeCode:        cnst.NO_VALUE,
		Expression:          cnst.NO_VALUE,
		Authors:             cnst.NO_VALUE,
		XdsPid:              cnst.NO_VALUE,
		XdsDocEntryUid:      cnst.NO_VALUE,
		RepositoryUniqueId:  cnst.NO_VALUE,
		NhsId:               cnst.NO_VALUE,
		User:                cnst.NO_VALUE,
		Org:                 cnst.NO_VALUE,
		Role:                cnst.NO_VALUE,
		Topic:               cnst.NO_VALUE,
		Pathway:             cnst.NO_VALUE,
		Notes:               "None",
		Version:             "0",
		BrokerRef:           dsubNotify.NotificationMessage.SubscriptionReference.Address.Text,
		XDWWorkflowDocument: XDWWorkflowDocument{},
	}
	if tukevent.BrokerRef == "" {
		return errors.New("no subscription ref found in notification message")
	}
	log.Printf("Found Subscription Reference %s. Setting Event state from Notify Message", tukevent.BrokerRef)
	tukevent.initTUKEvent(dsubNotify)

	log.Printf("Checking for event subscriptions with Broker Ref %s", tukevent.BrokerRef)
	subs := Subscriptions{Action: "select"}
	sub := Subscription{BrokerRef: tukevent.BrokerRef}
	subs.Subscriptions = append(subs.Subscriptions, sub)
	if err := subs.NewEvent(); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("Event Subscriptions Count : %v", subs.Count)
	if subs.Count > 0 {
		log.Printf("Found %s %s Subsription for Broker Ref %s", subs.Subscriptions[1].Pathway, subs.Subscriptions[1].Expression, tukevent.BrokerRef)
		tukevent.Pathway = subs.Subscriptions[1].Pathway
		tukevent.Topic = subs.Subscriptions[1].Topic
		log.Println("Registering DSUB Notification with Event Service")

		log.Printf("Obtaining NHS ID. Using %s", tukevent.XdsPid+":"+REGIONAL_OID)
		pixmQuery := PIXmQuery{PID: tukevent.XdsPid, PIDOID: REGIONAL_OID}
		if err := pixmQuery.InitPIXPatient(); err != nil {
			log.Println(err.Error())
			return err
		}
		if pixmQuery.Count != 1 {
			return errors.New("no unique patient returned")
		}
		evs := Events{
			Action: "insert",
		}
		tukevent.NhsId = pixmQuery.Response[0].NHSID
		if len(tukevent.NhsId) == 10 {
			log.Printf("Obtained NHS ID %s", tukevent.NhsId)
			evs.Events = append(evs.Events, tukevent)
			if err := evs.NewEvent(); err != nil {
				log.Println(err.Error())
			} else {
				log.Printf("Persisted Event ID %v", evs.LastInsertId)
			}
			log.Printf("Created TUK Event from DSUB Notification of the Publication of Document Type %s - Broker Ref - %s", tukevent.Expression, tukevent.BrokerRef)
			tukevent.EventId = evs.LastInsertId
			tukevent.updateWorkflow(pixmQuery.Response[0])
		} else {
			return errors.New("unable to obtain nhs id")
		}
	} else {
		log.Printf("No Subscription found with brokerref = %s. Sending Cancel request to Broker", tukevent.BrokerRef)
		cancel := DSUBCancel{BrokerRef: tukevent.BrokerRef, UUID: util.NewUuid()}
		cancel.NewEvent()
	}

	return nil
}
func (i *EventMessage) initDSUBNotifyMessage() (DSUBNotifyMessage, error) {
	dsubNotify := DSUBNotifyMessage{}
	if i.Message == "" {
		return dsubNotify, errors.New("message is empty")
	}
	notifyElement := util.GetXMLNodeList(i.Message, cnst.DSUB_NOTIFY_ELEMENT)
	if notifyElement == "" {
		return dsubNotify, errors.New("unable to locate notify element in received message")
	}
	log.Println("DSUB Broker Notify Element")
	log.Println(notifyElement)
	if err := xml.Unmarshal([]byte(notifyElement), &dsubNotify); err != nil {
		return dsubNotify, err
	}
	return dsubNotify, nil
}
func (i *Event) updateWorkflow(pat PIXPatient) {
	log.Printf("Updating Event Service %s Workflow for patient %s %s %s", i.Pathway, pat.GivenName, pat.FamilyName, i.NhsId)
	wfdefs := XDWS{Action: "select"}
	wfdef := XDW{
		Name: strings.ToUpper(i.Pathway),
	}
	wfdefs.XDW = append(wfdefs.XDW, wfdef)
	if err := wfdefs.NewEvent(); err != nil {
		log.Println(err.Error())
		return
	}
	if wfdefs.Count > 0 {
		log.Println("Found Workflow Definition for Pathway " + i.Pathway)
		wfdef := WorkflowDefinition{}
		if err := json.Unmarshal([]byte(wfdefs.XDW[1].XDW), &wfdef); err != nil {
			log.Println(err.Error())
			return
		}
		log.Println("Parsed Workflow Definition for Pathway " + wfdef.Ref)

		log.Printf("Searching for exisitng workflow for %s %s", strings.ToUpper(i.Pathway), i.NhsId)
		wfdocs := Workflows{
			Action:    "select",
			Workflows: []Workflow{},
		}
		wfdoc := Workflow{
			XDW_Key: strings.ToUpper(i.Pathway) + i.NhsId,
		}
		wfdocs.Workflows = append(wfdocs.Workflows, wfdoc)
		if err := wfdocs.NewEvent(); err != nil {
			log.Println(err.Error())
			return
		}
		if wfdocs.Count == 0 {
			log.Printf("No existing workflow state found for %s %s", strings.ToUpper(i.Pathway), i.NhsId)
			i.XDWWorkflowDocument = i.createWorkflow(wfdef, pat)
			log.Println("Creating Workflow state")
			var wfdocbytes []byte
			var wfdefbytes []byte
			var err error
			if wfdocbytes, err = json.Marshal(i.XDWWorkflowDocument); err != nil {
				log.Println(err.Error())
				return
			}
			if wfdefbytes, err = json.Marshal(wfdef); err != nil {
				log.Println(err.Error())
				return
			}
			wfdocstr := string(wfdocbytes)
			wfdefstr := string(wfdefbytes)
			wfdocs = Workflows{Action: "insert"}
			wfdoc = Workflow{
				XDW_Key:   strings.ToUpper(i.Pathway) + i.NhsId,
				XDW_UID:   i.XDWWorkflowDocument.ID.Extension,
				XDW_Doc:   wfdocstr,
				XDW_Def:   wfdefstr,
				Version:   0,
				Published: false,
			}
			wfdocs.Workflows = append(wfdocs.Workflows, wfdoc)
			if err := wfdocs.NewEvent(); err != nil {
				log.Println(err.Error())
				return
			}
			log.Println("Persisted Workflow state")
		} else {
			log.Printf("Existing Workflow state found for Pathway %s NHS ID %s", i.Pathway, i.NhsId)
			if err := json.Unmarshal([]byte(wfdocs.Workflows[1].XDW_Doc), &i.XDWWorkflowDocument); err != nil {
				log.Println(err.Error())
			}
			log.Printf("Updating %s Workflow for NHS ID %s with latest events", i.Pathway, i.NhsId)
			i.updateActiveWorkflow()
		}

	} else {
		log.Printf("Warning. No Event service XDW Definition found for pathway %s", i.Pathway)

	}
}
func (i *Event) updateActiveWorkflow() error {
	log.Println("Updating Active Workflow")

	if i.XDWWorkflowDocument.WorkflowStatus != "COMPLETE" {
		log.Println("Workflow is not complete. Updating Workflow Tasks")

		tukEvents := Events{Action: "select"}
		tukEvent := Event{Pathway: i.Pathway, NhsId: i.NhsId}
		tukEvents.Events = append(tukEvents.Events, tukEvent)
		if err := tukEvents.NewEvent(); err != nil {
			log.Println(err.Error())
			return err
		}
		i.Events = tukEvents
		sort.Sort(eventsList(i.Events.Events))
		log.Printf("Updating %s Workflow Tasks with %v Events", i.XDWWorkflowDocument.WorkflowDefinitionReference, len(i.Events.Events))

		log.Println("Replacing Active Workflow State with Updated Workflow State")

	}
	return nil
}

// func (i *Event) updateWorkflowTasks() error {
// 	tukEvents := Events{Action: "select"}
// 	tukEvent := Event{Pathway: i.Pathway, NhsId: i.NhsId}
// 	tukEvents.Events = append(tukEvents.Events, tukEvent)
// 	if err := tukEvents.NewEvent(); err != nil {
// 		return err
// 	}
// 	i.Events = tukEvents
// 	sort.Sort(eventsList(i.Events.Events))
// 	log.Printf("Updating %s Workflow Tasks with %v Events", i.XDWWorkflowDocument.WorkflowDefinitionReference, len(i.Events.Events))
// 	var newVers = false
// 	for _, ev := range i.Events.Events {
// 		for k, wfdoctask := range i.XDWWorkflowDocument.TaskList.XDWTask {
// 			log.Println("Checking Workflow Document Task " + wfdoctask.TaskData.TaskDetails.Name + " for matching Events")
// 			for inp, input := range wfdoctask.TaskData.Input {
// 				if ev.Expression == input.Part.Name {
// 					log.Println("Matched workflow document task " + wfdoctask.TaskData.TaskDetails.ID + " Input Part : " + input.Part.Name + " with Event Expression : " + ev.Expression + " Status : " + wfdoctask.TaskData.TaskDetails.Status)
// 					if !i.isInputRegistered(k, ev) {
// 						i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.LastModifiedTime = time.Now().Format(time.RFC3339)
// 						i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.AttachedTime = time.Now().Format(time.RFC3339)
// 						i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.AttachedBy = ev.User + " " + ev.Org + " " + ev.Role
// 						i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.Status = "REQUESTED"
// 						i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.ActualOwner = ev.User + " " + ev.Org + " " + ev.Role
// 						if strings.HasSuffix(wfdoctask.TaskData.Input[inp].Part.AttachmentInfo.AccessType, "XDSregistered") {
// 							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.Identifier = ev.RepositoryUniqueId + ":" + ev.XdsDocEntryUid
// 							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.HomeCommunityId, _ = tukdb.GetLocalId(constants.XDSDOMAIN)
// 							i.newTaskEvent(k, strconv.Itoa(ev.Id), ev.CreationTime, ev.Expression)
// 						} else {
// 							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.Identifier = strconv.Itoa(ev.Id)
// 							i.newTaskEvent(k, strconv.Itoa(ev.Id), ev.CreationTime, ev.Expression)
// 						}
// 						i.XDWWorkflowDocument.WorkflowStatus = "IN_PROGRESS"
// 					}
// 				}
// 			}
// 			for oup, output := range i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output {
// 				if ev.Expression == output.Part.Name {
// 					log.Println("Matched workflow document task " + wfdoctask.TaskData.TaskDetails.ID + " Output Part : " + output.Part.Name + " with Event Expression : " + ev.Expression + " Status : " + wfdoctask.TaskData.TaskDetails.Status)
// 					if !i.isOutputRegistered(k, ev) {
// 						i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.LastModifiedTime = time.Now().Format(time.RFC3339)
// 						i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.AttachedTime = time.Now().Format(time.RFC3339)
// 						i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.AttachedBy = ev.User + " " + ev.Org + " " + ev.Role
// 						i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.ActualOwner = ev.User + " " + ev.Org + " " + ev.Role
// 						i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.Status = "IN_PROGRESS"
// 						var tid = Newid()
// 						if strings.HasSuffix(wfdoctask.TaskData.Output[oup].Part.AttachmentInfo.AccessType, "XDSregistered") {
// 							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.Identifier = ev.RepositoryUniqueId + ":" + ev.XdsDocEntryUid
// 							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.HomeCommunityId, _ = tukdb.GetLocalId(constants.XDSDOMAIN)
// 							tid, newVers = i.newTaskEvent(k, strconv.Itoa(ev.Id), time.Now().Format(time.RFC3339), ev.Expression)
// 							if newVers {
// 								wfseqnum, _ := strconv.ParseInt(i.XDWWorkflowDocument.WorkflowDocumentSequenceNumber, 0, 0)
// 								wfseqnum = wfseqnum + 1
// 								i.XDWWorkflowDocument.WorkflowDocumentSequenceNumber = strconv.Itoa(int(wfseqnum))
// 								i.newDocEvent(ev, tid, k)
// 							}
// 						} else {
// 							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.Identifier = strconv.Itoa(ev.Id)
// 							tid, newVers = i.newTaskEvent(k, strconv.Itoa(ev.Id), time.Now().Format(time.RFC3339), ev.Expression)
// 							if newVers {
// 								wfseqnum, _ := strconv.ParseInt(i.XDWWorkflowDocument.WorkflowDocumentSequenceNumber, 0, 0)
// 								wfseqnum = wfseqnum + 1
// 								i.XDWWorkflowDocument.WorkflowDocumentSequenceNumber = strconv.Itoa(int(wfseqnum))
// 								i.newDocEvent(ev, tid, k)
// 							}
// 						}
// 						i.XDWWorkflowDocument.WorkflowStatus = "IN_PROGRESS"
// 					}
// 				}
// 			}
// 		}
// 	}
// 	for task := range i.XDWWorkflowDocument.TaskList.XDWTask {
// 		if i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskData.TaskDetails.Status != "COMPLETE" {
// 			if i.isTaskCompleteBehaviorMet(task) {
// 				i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskData.TaskDetails.Status = "COMPLETE"
// 			}
// 		}
// 	}
// 	for task := range i.XDWWorkflowDocument.TaskList.XDWTask {
// 		if i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskData.TaskDetails.Status != "COMPLETE" {
// 			if i.isTaskCompleteBehaviorMet(task) {
// 				i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskData.TaskDetails.Status = "COMPLETE"
// 			}
// 		}
// 	}
// 	if isWorkflowCompleteBehaviorMet(i) {
// 		i.XDWWorkflowDocument.WorkflowStatus = "COMPLETE"
// 		tevidstr := strconv.Itoa(int(i.newODDEvent("WORKFLOW", "CLOSE", "All Workflow Completion Behaviour Conditions Met. Workflow Closed")))
// 		docevent := DocumentEvent{}
// 		docevent.Author = i.User
// 		docevent.TaskEventIdentifier = tevidstr
// 		docevent.EventTime = i.Creationtime
// 		docevent.EventType = "CLOSE"
// 		docevent.PreviousStatus = i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent[len(i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent)-1].ActualStatus
// 		docevent.ActualStatus = "COMPLETE"
// 		i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent = append(i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent, docevent)
//			for k := range i.XDWWorkflowDocument.TaskList.XDWTask {
//				i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.Status = "COMPLETE"
//			}
//			log.Println("Closed Workflow. Total Workflow Document Events " + strconv.Itoa(len(i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent)))
//		}
//		return nil
//	}
// func (i *Event) isInputRegistered(ev Event, k int) bool {
// 	for _, input := range i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input {
// 		if (ev.Expression == input.Part.Name) && (input.Part.AttachmentInfo.AttachedBy == i.User+" "+i.Org) {
// 			log.Println("Event is already registered. Skipping Event ")
// 			return true
// 		}
// 	}
// 	log.Println("Processing New Event ")
// 	return false
// }
// func (i *Event) isOutputRegistered(k int) bool {
// 	for _, output := range i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output {
// 		if (i.Expression == output.Part.Name) && (output.Part.AttachmentInfo.AttachedBy == i.User+" "+i.Org) {
// 			log.Println("Event is already registered. Skipping Event ")
// 			return true
// 		}
// 	}
// 	log.Println("Processing New Event ")
// 	return false
// }
// func (i *Event) newDocEvent(tid string, k int) {
// 	docevent := DocumentEvent{}
// 	docevent.Author = i.User
// 	docevent.TaskEventIdentifier = tid
// 	docevent.EventTime = time.Now().Format(time.RFC3339)
// 	docevent.EventType = i.Expression
// 	docevent.PreviousStatus = i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent[len(i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent)-1].ActualStatus
// 	docevent.ActualStatus = "IN_PROGRESS"
// 	i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent = append(i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent, docevent)
// }
// func (i *Event) newTaskEvent(task int, evid string, evtime string, evtype string) (string, bool) {
//		for _, tev := range i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskEventHistory.TaskEvent {
//			if tev.ID == evid {
//				log.Println("Task Event Exists")
//				return tev.ID, false
//			}
//		}
//		tid64, _ := strconv.ParseInt(evid, 0, 0)
//		nextTaskEventId := strconv.Itoa(int(tid64))
//		nte := TaskEvent{
//			ID:         evid,
//			EventTime:  evtime,
//			Identifier: evid,
//			EventType:  evtype,
//			Status:     "COMPLETE",
//		}
//		i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskEventHistory.TaskEvent = append(i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskEventHistory.TaskEvent, nte)
//		return nextTaskEventId, true
//	}

func (i *Event) createWorkflow(xdwdef WorkflowDefinition, pat PIXPatient) XDWWorkflowDocument {
	log.Printf("Creating New %s Workflow for NHS ID %s", i.Pathway, i.NhsId)
	xdwdoc := XDWWorkflowDocument{}
	var authoroid = "Not Provided"
	var authorname = i.Org
	var wfid = util.Newid()
	if strings.Contains(i.Org, "^") {
		authoroid = strings.Split(i.Org, "^")[1]
		authorname = strings.Split(i.Org, "^")[0]
	}
	xdwdoc.Xdw = cnst.XDWNameSpace
	xdwdoc.Hl7 = cnst.HL7NameSpace
	xdwdoc.WsHt = cnst.WHTNameSpace
	xdwdoc.Xsi = cnst.XMLNS_XSI
	xdwdoc.XMLName.Local = cnst.XDWNameLocal
	xdwdoc.SchemaLocation = cnst.WorkflowDocumentSchemaLocation
	xdwdoc.ID.Root = strings.ReplaceAll(cnst.WorkflowInstanceId, "^", "")
	xdwdoc.ID.Extension = wfid
	xdwdoc.ID.AssigningAuthorityName = "ICS"
	xdwdoc.EffectiveTime.Value = i.Creationtime
	xdwdoc.ConfidentialityCode.Code = xdwdef.Confidentialitycode
	xdwdoc.Patient.ID.Root = NHS_OID
	xdwdoc.Patient.ID.Extension = i.NhsId
	xdwdoc.Patient.ID.AssigningAuthorityName = "NHS"
	xdwdoc.Author.AssignedAuthor.ID.Root = authoroid
	xdwdoc.Author.AssignedAuthor.ID.Extension = strings.ToUpper(authorname)
	xdwdoc.Author.AssignedAuthor.ID.AssigningAuthorityName = strings.ToUpper(authorname)
	xdwdoc.Author.AssignedAuthor.AssignedPerson.Name.Family = i.User
	xdwdoc.Author.AssignedAuthor.AssignedPerson.Name.Prefix = i.PracticeCode
	xdwdoc.WorkflowInstanceId = wfid + cnst.WorkflowInstanceId
	xdwdoc.WorkflowDocumentSequenceNumber = "1"
	xdwdoc.WorkflowStatus = "OPEN"
	xdwdoc.WorkflowDefinitionReference = strings.ToUpper(i.Pathway) + i.NhsId

	for _, t := range xdwdef.Tasks {
		task := XDWTask{}
		task.TaskData.TaskDetails.ID = t.ID
		task.TaskData.TaskDetails.TaskType = t.Tasktype
		task.TaskData.TaskDetails.Name = t.Name
		task.TaskData.TaskDetails.ActualOwner = t.Owner
		task.TaskData.TaskDetails.CreatedBy = i.User
		task.TaskData.TaskDetails.CreatedTime = i.Creationtime
		task.TaskData.TaskDetails.RenderingMethodExists = "false"
		task.TaskData.TaskDetails.LastModifiedTime = i.Creationtime
		task.TaskData.Description = t.Description
		task.TaskData.TaskDetails.Status = "CREATED"

		for _, inp := range t.Input {
			log.Println("Creating Task Input " + inp.Name)
			docinput := Input{}
			part := Part{}
			part.Name = inp.Name
			part.AttachmentInfo.Name = inp.Name
			part.AttachmentInfo.AccessType = inp.AccessType
			part.AttachmentInfo.ContentType = inp.Contenttype
			part.AttachmentInfo.ContentCategory = cnst.MEDIA_TYPES
			docinput.Part = part
			task.TaskData.Input = append(task.TaskData.Input, docinput)
		}
		for _, outp := range t.Output {
			log.Println("Creating Task Output " + outp.Name)
			docoutput := Output{}
			part := Part{}
			part.Name = outp.Name
			part.AttachmentInfo.Name = outp.Name
			part.AttachmentInfo.AccessType = outp.AccessType
			part.AttachmentInfo.ContentType = outp.Contenttype
			part.AttachmentInfo.ContentCategory = cnst.MEDIA_TYPES
			docoutput.Part = part
			task.TaskData.Output = append(task.TaskData.Output, docoutput)
		}
		tev := TaskEvent{}
		tev.EventTime = i.Creationtime
		tev.ID = t.ID
		tev.Identifier = strconv.Itoa(int(i.EventId))
		tev.EventType = "Create_Task"
		tev.Status = "COMPLETE"
		log.Println("Created Workflow Task Event Set 'Create_Task' ID " + tev.ID + " status to 'COMPLETE'")

		task.TaskEventHistory.TaskEvent = append(task.TaskEventHistory.TaskEvent, tev)
		xdwdoc.TaskList.XDWTask = append(xdwdoc.TaskList.XDWTask, task)
	}
	docevent := DocumentEvent{}
	docevent.Author = i.User + " (" + i.PracticeCode + " " + i.Org + ")"
	docevent.TaskEventIdentifier = strconv.Itoa(int(i.EventId))
	docevent.EventTime = i.Creationtime
	docevent.EventType = "Create_Workflow"
	docevent.PreviousStatus = ""
	docevent.ActualStatus = "OPEN"
	log.Println("Created Workflow Document Event Set 'New_Workflow' - status to 'OPEN'")
	xdwdoc.WorkflowStatusHistory.DocumentEvent = append(xdwdoc.WorkflowStatusHistory.DocumentEvent, docevent)

	log.Println("Created new " + xdwdoc.WorkflowDefinitionReference + " Workflow for Patient " + i.NhsId)
	b, _ := xml.MarshalIndent(xdwdoc, "", "  ")
	log.Println(string(b))
	return xdwdoc
}
func (i *Event) initTUKEvent(dsubNotify DSUBNotifyMessage) {
	var slots = dsubNotify.NotificationMessage.Message.SubmitObjectsRequest.RegistryObjectList.ExtrinsicObject
	log.Println("Event Creation Time " + i.Creationtime)
	log.Println("Set Document Name:" + i.DocName)

	log.Println("Searching for Repository Unique ID")
	for _, slot := range slots.Slot {
		if slot.Name == cnst.REPOSITORY_UID {
			i.RepositoryUniqueId = slot.ValueList.Value[0]
		}
	}
	log.Println("Set Repository Unique ID:" + i.RepositoryUniqueId)

	type Author struct {
		Person      string `json:"authorPerson"`
		Institution string `json:"authorInstitution"`
		Speciality  string `json:"authorSpeciality"`
		Role        string `json:"authorRole"`
	}
	type Authors struct {
		Author []Author `json:"authors"`
	}
	authors := Authors{}
	for _, c := range slots.Classification {
		log.Printf("Found Classification Scheme %s", c.ClassificationScheme)
		val := c.Name.LocalizedString.Value
		switch c.ClassificationScheme {
		case cnst.URN_CLASS_CODE:
			i.ClassCode = val
			log.Printf("Set ClassCode:%s", val)
		case cnst.URN_CONF_CODE:
			i.ConfCode = val
			log.Printf("Set ConfCode:%s", val)
		case cnst.URN_FORMAT_CODE:
			i.FormatCode = val
			log.Printf("Set FormatCode:%s", val)
		case cnst.URN_FACILITY_CODE:
			i.FacilityCode = val
			log.Printf("Set FacilityCode:%s", val)
		case cnst.URN_PRACTICE_CODE:
			i.PracticeCode = val
			log.Printf("Set PracticeCode:%s", val)
		case cnst.URN_TYPE_CODE:
			i.Expression = val
			log.Printf("Set TypeCode:%s", val)
		case cnst.URN_AUTHOR:
			author := Author{}
			for _, s := range c.Slot {
				switch s.Name {
				case cnst.AUTHOR_PERSON:
					for _, ap := range s.ValueList.Value {
						if len(ap) > 0 {
							if strings.Contains(ap, "^") {
								apsplit := strings.Split(ap, "^")
								if len(apsplit) > 2 {
									ap = apsplit[1] + " " + apsplit[2]
								} else {
									ap = apsplit[1]
								}
							}
							author.Person = author.Person + ap + ","
						}
					}
					author.Person = strings.TrimSuffix(author.Person, ",")
					log.Printf("Set Author Person:%s", author.Person)
				case cnst.AUTHOR_INSTITUTION:
					for _, ai := range s.ValueList.Value {
						if len(ai) > 0 {
							aioid := "null"
							ainame := ai
							if strings.Contains(ai, ":") && len(strings.Split(ai, ":")) > 2 {
								aioid = strings.Split(ai, ":")[2]
							}
							if strings.Contains(ai, "^") {
								ainame = strings.Split(ai, "^")[0]
							}

							author.Institution = author.Institution + ainame + "^" + aioid + ","
						}
					}
					author.Institution = strings.TrimSuffix(author.Institution, ",")
					log.Printf("Set Author Institution:%s", author.Institution)
				case cnst.AUTHOR_SPECIALITY:
					for _, as := range s.ValueList.Value {
						author.Speciality = author.Speciality + as + ","
					}
					author.Speciality = strings.TrimSuffix(author.Speciality, ",")
					log.Printf("Set Author Speciality:%s", author.Speciality)
				case cnst.AUTHOR_ROLE:
					for _, ar := range s.ValueList.Value {
						author.Role = author.Role + ar + ","
					}
					author.Role = strings.TrimSuffix(author.Role, ",")
					log.Printf("Set Author Role:%s", author.Role)
				}

			}
			authors.Author = append(authors.Author, author)
		default:
			log.Printf("Unknown classication scheme %s. Skipping", c.ClassificationScheme)
		}

	}
	bstr, _ := json.Marshal(authors)
	i.Authors = string(bstr)
	i.User = strings.ReplaceAll(authors.Author[0].Person, "^", " ")
	log.Printf("Set Event Author Person:%s", i.User)

	if strings.Contains(authors.Author[0].Institution, "^") {
		i.Org = strings.Split(authors.Author[0].Institution, "^")[0]
	} else {
		i.Org = authors.Author[0].Institution
	}
	log.Printf("Set Event Author Organisation:%s", i.Org)

	if authors.Author[0].Role != "" {
		i.Role = strings.ReplaceAll(authors.Author[0].Role, "^", " ")
	}
	log.Printf("Set Event Author Role:%s", i.Role)

	for exid := range slots.ExternalIdentifier {
		val := slots.ExternalIdentifier[exid].Value
		ids := slots.ExternalIdentifier[exid].IdentificationScheme
		switch ids {
		case cnst.URN_XDS_PID:
			i.XdsPid = strings.Split(val, "^^^")[0]
			log.Println("Set Regional ID:" + i.XdsPid)
		case cnst.URN_XDS_DOCUID:
			i.XdsDocEntryUid = val
			log.Println("Set XDS DOC UID:" + i.XdsDocEntryUid)
		}
	}
	log.Println("Parsed DSUB Notify Message")
}
func NewDSUBAcknowledgement() []byte {
	return []byte("<SOAP-ENV:Envelope xmlns:SOAP-ENV='http://www.w3.org/2003/05/soap-envelope' xmlns:s='http://www.w3.org/2001/XMLSchema' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'><SOAP-ENV:Body/></SOAP-ENV:Envelope>")
}
func (i *DSUBSubscribe) NewEvent() error {
	reqMsg := "{{define \"subscribe\"}}<SOAP-ENV:Envelope xmlns:SOAP-ENV='http://www.w3.org/2003/05/soap-envelope' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns:s='http://www.w3.org/2001/XMLSchema' xmlns:wsa='http://www.w3.org/2005/08/addressing'><SOAP-ENV:Header><wsa:Action SOAP-ENV:mustUnderstand='true'>http://docs.oasis-open.org/wsn/bw-2/NotificationProducer/SubscribeRequest</wsa:Action><wsa:MessageID>urn:uuid:{{newuuid}}</wsa:MessageID><wsa:ReplyTo SOAP-ENV:mustUnderstand='true'><wsa:Address>http://www.w3.org/2005/08/addressing/anonymous</wsa:Address></wsa:ReplyTo><wsa:To>{{.BrokerUrl}}</wsa:To></SOAP-ENV:Header><SOAP-ENV:Body><wsnt:Subscribe xmlns:wsnt='http://docs.oasis-open.org/wsn/b-2' xmlns:a='http://www.w3.org/2005/08/addressing' xmlns:rim='urn:oasis:names:tc:ebxml-regrep:xsd:rim:3.0' xmlns:wsa='http://www.w3.org/2005/08/addressing'><wsnt:ConsumerReference><wsa:Address>{{.ConsumerUrl}}</wsa:Address></wsnt:ConsumerReference><wsnt:Filter><wsnt:TopicExpression Dialect='http://docs.oasis-open.org/wsn/t-1/TopicExpression/Simple'>ihe:FullDocumentEntry</wsnt:TopicExpression><rim:AdhocQuery id='urn:uuid:742790e0-aba6-43d6-9f1f-e43ed9790b79'><rim:Slot name='{{.Topic}}'><rim:ValueList><rim:Value>('{{.Expression}}')</rim:Value></rim:ValueList></rim:Slot></rim:AdhocQuery></wsnt:Filter></wsnt:Subscribe></SOAP-ENV:Body></SOAP-ENV:Envelope>{{end}}"
	tmplt, err := template.New("subscribe").Parse(reqMsg)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	var b bytes.Buffer
	err = tmplt.Execute(&b, i)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	i.Request = b.Bytes()
	err = i.createSubscription()
	if err != nil {
		log.Println(err.Error())
	}
	return err
}
func (i *DSUBSubscribe) createSubscription() error {
	if i.BrokerUrl == "" {
		i.BrokerUrl = DSUB_BROKER_URL
	}
	var resp *http.Response
	req, err := http.NewRequest(http.MethodPost, i.BrokerUrl, strings.NewReader(string(i.Request)))
	if err != nil {
		log.Println(err.Error())
		return err
	}
	req.Header.Set(cnst.SOAP_ACTION, cnst.SOAP_ACTION_SUBSCRIBE_REQUEST)
	req.Header.Set(cnst.CONTENT_TYPE, cnst.SOAP_XML)
	req.Header.Set(cnst.ACCEPT, cnst.ALL)
	req.Header.Set(cnst.CONNECTION, cnst.KEEP_ALIVE)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5000))
	defer cancel()
	resp, err = http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		log.Println(err.Error())
		return err
	}
	rsp, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	subrsp := DSUBSubscribeResponse{}
	if err := xml.Unmarshal(rsp, &subrsp); err != nil {
		log.Println(err.Error())
	}
	i.BrokerRef = subrsp.Body.SubscribeResponse.SubscriptionReference.Address
	log.Printf("Broker Response. Broker Ref :  %s", subrsp.Body.SubscribeResponse.SubscriptionReference.Address)
	return err
}
func (i *DSUBCancel) NewEvent() error {
	reqMsg := "{{define \"cancel\"}}<soap:Envelope xmlns:soap='http://www.w3.org/2003/05/soap-envelope'><soap:Header><Action xmlns='http://www.w3.org/2005/08/addressing' soap:mustUnderstand='true'>http://docs.oasis-open.org/wsn/bw-2/SubscriptionManager/UnsubscribeRequest</Action><MessageID xmlns='http://www.w3.org/2005/08/addressing' soap:mustUnderstand='true'>urn:uuid:{{.UUID}}</MessageID><To xmlns='http://www.w3.org/2005/08/addressing' soap:mustUnderstand='true'>{{.BrokerRef}}</To><ReplyTo xmlns='http://www.w3.org/2005/08/addressing' soap:mustUnderstand='true'><Address>http://www.w3.org/2005/08/addressing/anonymous</Address></ReplyTo></soap:Header><soap:Body><Unsubscribe xmlns='http://docs.oasis-open.org/wsn/b-2' xmlns:ns2='http://www.w3.org/2005/08/addressing' xmlns:ns3='http://docs.oasis-open.org/wsrf/bf-2' xmlns:ns4='urn:oasis:names:tc:ebxml-regrep:xsd:rim:3.0' xmlns:ns5='urn:oasis:names:tc:ebxml-regrep:xsd:rs:3.0' xmlns:ns6='urn:oasis:names:tc:ebxml-regrep:xsd:lcm:3.0' xmlns:ns7='http://docs.oasis-open.org/wsn/t-1' xmlns:ns8='http://docs.oasis-open.org/wsrf/r-2'/></soap:Body></soap:Envelope>{{end}}"
	tmplt, err := template.New("cancel").Parse(reqMsg)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	var b bytes.Buffer
	err = tmplt.Execute(&b, i)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	i.Request = b.Bytes()
	err = i.cancelSubscription()
	if err != nil {
		log.Println(err.Error())
	}
	return err
}
func (i *DSUBCancel) cancelSubscription() error {
	req, err := http.NewRequest(http.MethodPost, DSUB_BROKER_URL, strings.NewReader(string(i.Request)))
	if err != nil {
		log.Println(err.Error())
		return err
	}
	req.Header.Set(cnst.SOAP_ACTION, cnst.SOAP_ACTION_UNSUBSCRIBE_REQUEST)
	req.Header.Set(cnst.CONTENT_TYPE, cnst.SOAP_XML)
	req.Header.Set(cnst.ACCEPT, cnst.ALL)
	req.Header.Set(cnst.CONNECTION, cnst.KEEP_ALIVE)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5000))
	defer cancel()
	_, err = http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		log.Println(err.Error())
		return err
	}
	return err
}
func (i *PIXmQuery) InitPIXPatient() error {
	url := PIX_MANAGER_URL + "?identifier=" + i.PIDOID + "%7C" + i.PID + "&_format=json&_pretty=true"
	log.Println("GET Patient URL:" + url)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set(cnst.CONTENT_TYPE, cnst.APPLICATION_JSON)
	req.Header.Set(cnst.ACCEPT, cnst.ALL)
	req.Header.Set(cnst.CONNECTION, cnst.KEEP_ALIVE)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(2000)*time.Millisecond)
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		log.Println(err.Error())
		return err
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer resp.Body.Close()

	log.Println("Received PIXm Response")
	log.Println(string(b))
	if strings.Contains(string(b), "Error") {
		log.Println(string(b))
		return errors.New(string(b))
	}

	rsp := PIXmResponse{}
	if err := json.Unmarshal(b, &rsp); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("%v Patient Entries in Response", rsp.Total)
	i.Count = rsp.Total
	if i.Count > 0 {
		for cnt := 0; cnt < len(rsp.Entry); cnt++ {
			rsppat := rsp.Entry[cnt]
			tukpat := PIXPatient{}
			for _, id := range rsppat.Resource.Identifier {
				if id.System == "urn:oid:"+REGIONAL_OID {
					tukpat.REGID = id.Value
					tukpat.REGOID = REGIONAL_OID
					log.Printf("Set Reg ID %s %s", tukpat.REGID, tukpat.REGOID)
				}
				if id.Use == "usual" {
					tukpat.PID = id.Value
					tukpat.PIDOID = strings.Split(id.System, ":")[2]
					log.Printf("Set PID %s %s", tukpat.PID, tukpat.PIDOID)
				}
				if id.System == "urn:oid:"+NHS_OID {
					tukpat.NHSID = id.Value
					tukpat.NHSOID = NHS_OID
					log.Printf("Set NHS ID %s %s", tukpat.NHSID, tukpat.NHSOID)
				}
			}
			gn := ""
			for _, name := range rsppat.Resource.Name {
				for _, n := range name.Given {
					gn = gn + n + " "
				}
			}

			tukpat.GivenName = strings.TrimSuffix(gn, " ")
			tukpat.FamilyName = rsppat.Resource.Name[0].Family
			tukpat.BirthDate = strings.ReplaceAll(rsppat.Resource.BirthDate, "-", "")
			tukpat.Gender = rsppat.Resource.Gender

			if len(rsppat.Resource.Address) > 0 {
				tukpat.Zip = rsppat.Resource.Address[0].PostalCode
				tukpat.Street = rsppat.Resource.Address[0].Line[0]
				if len(rsppat.Resource.Address[0].Line) > 1 {
					tukpat.Town = rsppat.Resource.Address[0].Line[1]
				}
				tukpat.City = rsppat.Resource.Address[0].City
				tukpat.Country = rsppat.Resource.Address[0].Country
			}
			i.Response = append(i.Response, tukpat)
			log.Printf("Added Patient %s to response", tukpat.NHSID)
		}
	} else {
		log.Println("patient is not registered")
	}
	return nil
}
func (i *XDWS) NewEvent() error {
	log.Printf("Sending %s Request to %s", getHttpMethod(i.Action), TUK_DB_URL+"xdws")
	body, _ := json.Marshal(i)
	bodyBytes, err := newTUKDBRequest(getHttpMethod(i.Action), "xdws", body)
	if err == nil {
		if err := json.Unmarshal(bodyBytes, &i); err != nil {
			fmt.Println(err.Error())
		}
	}
	return err
}
func (i *Workflows) NewEvent() error {
	log.Printf("Sending %s Request to %s", getHttpMethod(i.Action), TUK_DB_URL+"workflows")
	body, _ := json.Marshal(i)
	bodyBytes, err := newTUKDBRequest(getHttpMethod(i.Action), "workflows", body)
	if err == nil {
		if err := json.Unmarshal(bodyBytes, &i); err != nil {
			fmt.Println(err.Error())
		}
	}
	return err
}
func (i *Subscriptions) NewEvent() error {
	log.Printf("Sending %s Request to %s", getHttpMethod(i.Action), TUK_DB_URL+"subscriptions")
	body, _ := json.Marshal(i)
	bodyBytes, err := newTUKDBRequest(getHttpMethod(i.Action), "subscriptions", body)
	if err == nil {
		if err := json.Unmarshal(bodyBytes, &i); err != nil {
			fmt.Println(err.Error())
		}
	}
	return err
}
func (i *Events) NewEvent() error {
	log.Printf("Sending %s Request to %s", getHttpMethod(i.Action), TUK_DB_URL+"events")
	body, _ := json.Marshal(i)
	bodyBytes, err := newTUKDBRequest(getHttpMethod(i.Action), "events", body)
	if err == nil {
		if err := json.Unmarshal(bodyBytes, &i); err != nil {
			fmt.Println(err.Error())
		}
	}
	return err
}
func (i *IDMaps) NewEvent() error {
	log.Printf("Sending %s Request to %s", getHttpMethod(i.Action), TUK_DB_URL+"idmaps")
	body, _ := json.Marshal(i)
	bodyBytes, err := newTUKDBRequest(getHttpMethod(i.Action), "idmaps", body)
	if err == nil {
		if err := json.Unmarshal(bodyBytes, &i); err != nil {
			fmt.Println(err.Error())
		}
	}
	return err
}
func getHttpMethod(action string) string {
	switch action {
	case "select":
		return "GET"
	default:
		return "POST"
	}
}
func newTUKDBRequest(httpMethod string, resource string, body []byte) ([]byte, error) {
	if TUK_DB_URL == "" {
		TUK_DB_URL = "https://5k2o64mwt5.execute-api.eu-west-1.amazonaws.com/beta/"
	}
	client := &http.Client{}
	req, err := http.NewRequest(httpMethod, TUK_DB_URL+resource, bytes.NewBuffer(body))
	if err != nil {
		log.Println(err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json; charset=utf-8")
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	log.Printf("Response Status Code %v\n", resp.StatusCode)
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		log.Println(string(bodyBytes))
		if err != nil {
			log.Println(err)
		} else {
			return bodyBytes, nil
		}
	}
	return nil, err
}

type eventsList []Event

func (e eventsList) Len() int {
	return len(e)
}
func (e eventsList) Less(i, j int) bool {
	return e[i].EventId > e[j].EventId
}
func (e eventsList) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
