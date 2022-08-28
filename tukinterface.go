package tukinterface

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	cnst "github.com/ipthomas/tukcnst"
	utils "github.com/ipthomas/tukutils"
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
	BrokerRef   string
	UUID        string
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
type ClientRequest struct {
	User       string `json:"user"`
	Org        string `json:"org"`
	Orgoid     string `json:"orgoid"`
	Role       string `json:"role"`
	NHS        string `json:"nhs"`
	PID        string `json:"pid"`
	PIDOrg     string `json:"pidorg"`
	PIDOID     string `json:"pidoid"`
	FamilyName string `json:"familyname"`
	GivenName  string `json:"givenname"`
	DOB        string `json:"dob"`
	Gender     string `json:"gender"`
	ZIP        string `json:"zip"`
	Status     string `json:"status"`
	XDWKey     string `json:"xdwkey"`
	ID         int    `json:"id"`
	Task       string `json:"task"`
	Pathway    string `json:"pathway"`
	Version    int    `json:"version"`
	ReturnXML  bool   `json:"returnxml"`
	Act        string `json:"act"`
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
type Workflow struct {
	Id        int    `json:"id"`
	Created   string `json:"created"`
	XDW_Key   string `json:"xdw_key"`
	XDW_UUID  string `json:"xdw_uid"`
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
type PIXPatient struct {
	Count      int    `json:"count"`
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

var (
	HostName, _       = os.Hostname()
	LogFile           *os.File
	cs                = make(map[string]string)
	TUKTemplates      *template.Template
	TUK_DB_URL        = ""
	DSUB_CONSUMER_URL = ""
	DSUB_BROKER_URL   = ""
	PIXm_URL          = ""
	REGIONAL_OID      = ""
	NHS_OID           = ""
)

func NewTUKService(basepath string) (TUKServiceState, error) {
	srvc := TUKServiceState{}
	pathToConfigFile := basepath + "/service.json"
	log.Printf("Loading Service State from %s", pathToConfigFile)
	file, err := os.Open(pathToConfigFile)
	if err == nil {
		json.NewDecoder(file).Decode(&srvc)
		log.Printf("Loaded Service State from %s", pathToConfigFile)
		srvc.BaseFolder = basepath
		TUK_DB_URL = srvc.TUK_DB_URL
		DSUB_BROKER_URL = srvc.DSUB_Broker_URL
		DSUB_CONSUMER_URL = srvc.DSUB_Consumer_URL
		err = srvc.initState()
	}

	return srvc, err
}
func NewClientRequest(r *http.Request) ClientRequest {
	log.Printf("Received http %s request", r.Method)

	r.ParseForm()
	req := ClientRequest{
		Act:        r.FormValue("act"),
		User:       r.FormValue("user"),
		Org:        r.FormValue("org"),
		Orgoid:     getCSVal(r.FormValue("org")),
		Role:       r.FormValue("role"),
		NHS:        r.FormValue("nhs"),
		PID:        r.FormValue("pid"),
		PIDOrg:     r.FormValue("pidorg"),
		PIDOID:     getCSVal(r.FormValue("pidorg")),
		FamilyName: r.FormValue("familyname"),
		GivenName:  r.FormValue("givenname"),
		DOB:        r.FormValue("dob"),
		Gender:     r.FormValue("gender"),
		ZIP:        r.FormValue("zip"),
		Status:     r.FormValue("status"),
		ID:         utils.StringToInt(r.FormValue("id")),
		Task:       r.FormValue("task"),
		Pathway:    r.FormValue("pathway"),
		Version:    utils.StringToInt(r.FormValue("version")),
		XDWKey:     r.FormValue("xdwkey"),
	}

	if r.Header.Get(cnst.ACCEPT) == cnst.APPLICATION_XML {
		req.ReturnXML = true
	}
	res2B, _ := json.MarshalIndent(req, "", "  ")
	log.Printf("Client Request\n%+v", string(res2B))
	return req
}
func NewDSUBEvent(eventMessage string) error {
	v, err := ValidateNotifyMessage(eventMessage)
	if err != nil {
		return err
	}
	v.setOSVars()
	var slots = v.NotificationMessage.Message.SubmitObjectsRequest.RegistryObjectList.ExtrinsicObject
	location, err := time.LoadLocation("Europe/London")
	if err != nil {
		log.Println(err.Error())
		return err
	}
	timeInUTC := time.Now().In(location).String()

	i := Event{
		EventId:             0,
		Creationtime:        timeInUTC,
		DocName:             slots.Name.LocalizedString.Value,
		ClassCode:           cnst.NO_VALUE,
		ConfCode:            NO_VALUE,
		FormatCode:          NO_VALUE,
		FacilityCode:        NO_VALUE,
		PracticeCode:        NO_VALUE,
		Expression:          NO_VALUE,
		Authors:             NO_VALUE,
		XdsPid:              NO_VALUE,
		XdsDocEntryUid:      NO_VALUE,
		RepositoryUniqueId:  NO_VALUE,
		NhsId:               NO_VALUE,
		User:                NO_VALUE,
		Org:                 NO_VALUE,
		Role:                NO_VALUE,
		Topic:               NO_VALUE,
		Pathway:             NO_VALUE,
		Notes:               "None",
		Version:             "0",
		BrokerRef:           v.NotificationMessage.SubscriptionReference.Address.Text,
		XDWWorkflowDocument: XDWWorkflowDocument{},
	}
	if i.BrokerRef == "" {
		return errors.New("no subscription ref found in notification message")
	}
	log.Printf("Found Subscription Reference %s. Setting Event state from Notify Message", i.BrokerRef)
	log.Println("Event Creation Time " + i.Creationtime)
	log.Println("Set Document Name:" + i.DocName)

	i.populateTUKEvent(v)

	log.Printf("Checking for event subscriptions with Broker Ref %s", i.BrokerRef)
	subs := Subscriptions{Action: "select"}
	sub := Subscription{BrokerRef: i.BrokerRef}
	subs.Subscriptions = append(subs.Subscriptions, sub)
	if err := subs.NewEvent(); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("Event Subscriptions Count : %v", subs.Count)
	if subs.Count > 0 {
		log.Printf("Found %s %s Subsription for Broker Ref %s", subs.Subscriptions[1].Pathway, subs.Subscriptions[1].Expression, i.BrokerRef)
		i.Pathway = subs.Subscriptions[1].Pathway
		i.Topic = subs.Subscriptions[1].Topic
		log.Println("Registering DSUB Notification with Event Service")

		log.Printf("Obtaining NHS ID. Using %s", i.XdsPid+":"+REGIONAL_OID)
		pat := PIXPatient{PID: i.XdsPid, PIDOID: REGIONAL_OID}
		if err := pat.NewEvent(); err != nil {
			log.Println(err.Error())
			return err
		}
		evs := Events{
			Action: "insert",
		}
		i.NhsId = pat.NHSID
		if len(i.NhsId) == 10 {
			log.Printf("Obtained NHS ID %s", i.NhsId)
			evs.Events = append(evs.Events, i)
			if err := evs.NewEvent(); err != nil {
				log.Println(err.Error())
			} else {
				log.Printf("Persisted Event ID %v", evs.LastInsertId)
			}
			log.Printf("Created TUK Event from DSUB Notification of the Publication of Document Type %s - Broker Ref - %s", i.Expression, i.BrokerRef)
			i.EventId = evs.LastInsertId
			i.updateEventService(pat)
		} else {
			return errors.New("unable to obtain nhs id")
		}
	} else {
		log.Printf("No Subscription found with brokerref = %s. Sending Cancel request to Broker", i.BrokerRef)
		cancel := DSUBCancel{BrokerRef: i.BrokerRef, UUID: NewUuid()}
		cancel.NewEvent()
	}

	return nil
}
func NewDSUBAcknowledgement() []byte {
	return []byte("<SOAP-ENV:Envelope xmlns:SOAP-ENV='http://www.w3.org/2003/05/soap-envelope' xmlns:s='http://www.w3.org/2001/XMLSchema' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'><SOAP-ENV:Body/></SOAP-ENV:Envelope>")
}
func (i *DSUBNotifyMessage) setOSVars() {

}
func (i *PIXPatient) NewEvent() error {
	url := PIX_MANAGER_URL + "?identifier=" + i.PIDOID + "%7C" + i.PID + "&_format=json&_pretty=true"
	log.Println("GET Patient URL:" + url)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set(cnst.CONTENT_TYPE, cnst.APPLICATION_JSON)
	req.Header.Set(cnst.ACCEPT, cnst.ALL)
	req.Header.Set(cnst.CONNECTION, cnst.KEEP_ALIVE)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(2000)*time.Millisecond)
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	//resp, err := http.DefaultClient.Do(req)
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
		pat := rsp.Entry[0]
		for _, id := range pat.Resource.Identifier {
			if id.System == "urn:oid:"+REGIONAL_OID {
				i.REGID = id.Value
				i.REGOID = REGIONAL_OID
				log.Printf("Set Reg ID %s %s", i.REGID, i.REGOID)
			}
			if id.Use == "usual" {
				i.PID = id.Value
				i.PIDOID = strings.Split(id.System, ":")[2]
				log.Printf("Set PID %s %s", i.PID, i.PIDOID)
			}
			if id.System == "urn:oid:"+NHS_OID {
				i.NHSID = id.Value
				i.NHSOID = NHS_OID
				log.Printf("Set NHS ID %s %s", i.NHSID, i.NHSOID)
			}
		}
		gn := ""
		for _, name := range pat.Resource.Name {
			for _, n := range name.Given {
				gn = gn + n + " "
			}
		}

		i.GivenName = strings.TrimSuffix(gn, " ")
		i.FamilyName = pat.Resource.Name[0].Family
		i.BirthDate = strings.ReplaceAll(pat.Resource.BirthDate, "-", "")
		i.Gender = pat.Resource.Gender

		if len(pat.Resource.Address) > 0 {
			i.Zip = pat.Resource.Address[0].PostalCode
			i.Street = pat.Resource.Address[0].Line[0]
			if len(pat.Resource.Address[0].Line) > 1 {
				i.Town = pat.Resource.Address[0].Line[1]
			}
			i.City = pat.Resource.Address[0].City
			i.Country = pat.Resource.Address[0].Country
		}

	} else {
		err = errors.New("patient is not registered")
	}
	return err
}
func (srvc *TUKServiceState) initState() error {
	var err error
	if err = srvc.initLogService(); err == nil {
		srvc.initCodeSystem()
		srvc.initTemplates()

	}
	return err
}
func (i *TUKServiceState) initLogService() error {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	if i.LogEnabled {
		mdir := i.BaseFolder + "/" + i.LogFolder
		if _, err := os.Stat(mdir); errors.Is(err, fs.ErrNotExist) {
			if e2 := os.Mkdir(mdir, 0700); e2 != nil {
				log.Println(err.Error())
				return e2
			}
		}
		dir := mdir + "/" + utils.Tuk_Year()
		if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
			if e2 := os.Mkdir(dir, 0700); e2 != nil {
				log.Println(err.Error())
				return e2
			}
		}
		LogFile, err := os.OpenFile(dir+"/"+utils.Tuk_Month()+utils.Tuk_Day()+".log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Println(err.Error())
			return err
		}
		log.Println("Using log file - " + LogFile.Name())
		log.SetOutput(LogFile)
		log.Println("-----------------------------------------------------------------------------------")
	}
	return nil
}
func (i *TUKServiceState) initCodeSystem() {
	var err error
	if csfile, err := os.OpenFile(i.BaseFolder+"/"+i.ConfigFolder+"/cs.json", os.O_RDONLY, 0666); err == nil {
		json.NewDecoder(csfile).Decode(&cs)
		log.Printf("Cached %v Code System Key Values", len(cs))
	}
	if err != nil {
		log.Println(err.Error())
	}
}
func (i *TUKServiceState) initTemplates() {
	var err error
	TUKTemplates, err = template.New("tuktemplates").Funcs(newTemplateFuncMap()).ParseGlob(i.BaseFolder + "/" + i.TemplatesFolder + "/*.*")
	if err != nil {
		log.Println(err.Error())
	}
}

// RegisterWorkflows
// Takes inputs of the executable basepath and the configfolder (sub folder of basepath) of the creator client and log service json config files
//
// It loads XDW definition files (located in the folder defined in the service json config) and parses the XDW definations to identify and create IHE DSUB Broker subscriptions that are required to trigger the workflow tasks in each workflow definition. When all subscrions have been created, it registers the subscription and XDW defintions with the TUK Event Service
func (i *TUKServiceState) RegisterWorkflows() (Subscriptions, error) {
	var err error
	var rspSubs = Subscriptions{}
	log.Println("Processing Workflow Definitions")
	rspSubs, err = i.newWorkflowsFromFiles()

	LogFile.Close()
	return rspSubs, err
}

func (i *TUKServiceState) newWorkflowsFromFiles() (Subscriptions, error) {
	var folderfiles []fs.DirEntry
	var file fs.DirEntry
	var err error
	var rspSubs = Subscriptions{}
	if folderfiles, err = i.getFolderFiles(); err == nil {
		for _, file = range folderfiles {
			if xdwdef, xdwbytes, err := i.newWorkflowDefinitionFromFile(file); err == nil {
				log.Printf("Deleting TUK Event Subscriptions for %s workflow", xdwdef.Ref)
				activesubs := Subscriptions{Action: cnst.DELETE}
				activesub := Subscription{
					Pathway: xdwdef.Ref,
				}
				activesubs.Subscriptions = append(activesubs.Subscriptions, activesub)
				body, _ := json.Marshal(activesubs)
				if _, err = newTUKHTTPDBRequest(cnst.SUBSCRIPTIONS, body); err != nil {
					log.Println(err.Error())
					return rspSubs, err
				}
				log.Printf("Deleted TUK Event Subscriptions for %s workflow", xdwdef.Ref)

				log.Printf("Deleting TUK Workflow Definition for %s workflow", xdwdef.Ref)
				activexdws := TUKXDWS{Action: cnst.DELETE}
				activexdw := TUKXDW{
					Name: xdwdef.Ref,
				}
				activexdws.XDW = append(activexdws.XDW, activexdw)
				body, _ = json.Marshal(activexdws)
				if _, err = newTUKHTTPDBRequest(cnst.XDWS, body); err != nil {
					log.Println(err.Error())
					return rspSubs, err
				}
				log.Printf("Deleted TUK Workflow Definition for %s workflow", xdwdef.Ref)

				pwExps := getXDWBrokerExpressions(xdwdef)

				if rspSubs, err = createSubscriptionsFromBrokerExpressions(pwExps); err == nil {
					var xdwdefBytes = make(map[string][]byte)
					xdwdefBytes[xdwdef.Ref] = xdwbytes
					persistXDWDefinitions(xdwdefBytes)
				}
			}
		}
	}
	if err != nil {
		log.Println(err.Error())
	}
	return rspSubs, err
}
func (i *TUKServiceState) newWorkflowDefinitionFromFile(file fs.DirEntry) (WorkflowDefinition, []byte, error) {
	xdwdef := WorkflowDefinition{}
	var xdwdefBytes []byte
	if strings.HasSuffix(file.Name(), ".json") && strings.Contains(file.Name(), cnst.XDW_DEFINITION_FILE) {
		input := i.BaseFolder + "/" + i.ConfigFolder + "/" + file.Name()

		xdwfile, err := os.Open(input)
		if err != nil {
			log.Println(err.Error())
			return xdwdef, xdwdefBytes, err
		}
		json.NewDecoder(xdwfile).Decode(&xdwdef)
		xdwdefBytes, err = json.MarshalIndent(xdwdef, "", "  ")
		if err != nil {
			log.Println(err.Error())
			return xdwdef, xdwdefBytes, err
		}

		log.Printf("Loaded WF Def for Pathway %s : Bytes = %v", xdwdef.Ref, len(xdwdefBytes))
	}
	return xdwdef, xdwdefBytes, nil
}
func ValidateNotifyMessage(eventMessage string) (DSUBNotifyMessage, error) {
	v := DSUBNotifyMessage{}
	if eventMessage == "" {
		return v, errors.New("body is empty")
	}
	notifyElement := GetXMLNodeList(eventMessage, cnst.DSUB_NOTIFY_ELEMENT)
	if notifyElement == "" {
		return v, errors.New("unable to locate notify element")
	}
	log.Println(notifyElement)

	if err := xml.Unmarshal([]byte(notifyElement), &v); err != nil {
		return v, err
	}
	return v, nil
}
func CreateSubscriptionsFromBrokerExpressions(brokerExps map[string]string) (Subscriptions, error) {
	log.Printf("Creating %v Broker Subscription", len(brokerExps))
	var err error
	var rspSubs = Subscriptions{Action: cnst.INSERT}
	for exp, pwy := range brokerExps {
		log.Printf("Creating Broker Subscription for %s workflow expression %s", pwy, exp)

		dsub := DSUBSubscribe{
			Topic:      cnst.DSUB_TOPIC_TYPE_CODE,
			Expression: exp,
		}
		if err = dsub.newEvent(); err != nil {
			log.Println(err.Error())
			return rspSubs, err
		}
		if dsub.BrokerRef != "" {
			tuksub := Subscription{
				BrokerRef:  dsub.BrokerRef,
				Pathway:    pwy,
				Topic:      cnst.DSUB_TOPIC_TYPE_CODE,
				Expression: exp,
			}
			tuksubs := Subscriptions{Action: cnst.INSERT}
			tuksubs.Subscriptions = append(tuksubs.Subscriptions, tuksub)
			log.Println("Registering Subscription Reference with Event Service")
			if err = tuksubs.newEvent(); err != nil {
				log.Println(err.Error())
			} else {
				rspSubs.Subscriptions = append(rspSubs.Subscriptions, tuksub)
			}
		}
	}
	return rspSubs, err
}
func (i *DSUBSubscribe) NewEvent() error {
	log.Printf("Creating DSUB Subscribe message for Topic %s - Expression %s", i.Topic, i.Expression)
	var err error
	var tmplt *template.Template
	i.UUID = utils.NewUuid()
	i.BrokerUrl = DSUB_BROKER_URL
	i.ConsumerUrl = DSUB_CONSUMER_URL
	reqMsg := "{{define \"subscribe\"}}<SOAP-ENV:Envelope xmlns:SOAP-ENV='http://www.w3.org/2003/05/soap-envelope' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns:s='http://www.w3.org/2001/XMLSchema' xmlns:wsa='http://www.w3.org/2005/08/addressing'><SOAP-ENV:Header><wsa:Action SOAP-ENV:mustUnderstand='true'>http://docs.oasis-open.org/wsn/bw-2/NotificationProducer/SubscribeRequest</wsa:Action><wsa:MessageID>urn:uuid:{{.UUID}}</wsa:MessageID><wsa:ReplyTo SOAP-ENV:mustUnderstand='true'><wsa:Address>http://www.w3.org/2005/08/addressing/anonymous</wsa:Address></wsa:ReplyTo><wsa:To>{{.BrokerUrl}}</wsa:To></SOAP-ENV:Header><SOAP-ENV:Body><wsnt:Subscribe xmlns:wsnt='http://docs.oasis-open.org/wsn/b-2' xmlns:a='http://www.w3.org/2005/08/addressing' xmlns:rim='urn:oasis:names:tc:ebxml-regrep:xsd:rim:3.0' xmlns:wsa='http://www.w3.org/2005/08/addressing'><wsnt:ConsumerReference><wsa:Address>{{.ConsumerUrl}}</wsa:Address></wsnt:ConsumerReference><wsnt:Filter><wsnt:TopicExpression Dialect='http://docs.oasis-open.org/wsn/t-1/TopicExpression/Simple'>ihe:FullDocumentEntry</wsnt:TopicExpression><rim:AdhocQuery id='urn:uuid:742790e0-aba6-43d6-9f1f-e43ed9790b79'><rim:Slot name='{{.Topic}}'><rim:ValueList><rim:Value>('{{.Expression}}')</rim:Value></rim:ValueList></rim:Slot></rim:AdhocQuery></wsnt:Filter></wsnt:Subscribe></SOAP-ENV:Body></SOAP-ENV:Envelope>{{end}}"
	if tmplt, err = template.New(cnst.SUBSCRIBE).Parse(reqMsg); err == nil {
		var b bytes.Buffer
		if err = tmplt.Execute(&b, i); err == nil {
			log.Printf("Creating DSUB Subscribe request - Broker URL %s", i.BrokerUrl)
			var resp *http.Response
			var rsp []byte
			if resp, err = newSOAPRequest(DSUB_BROKER_URL, cnst.SOAP_ACTION_SUBSCRIBE_REQUEST, b.String(), 5000); err == nil {
				rsp, err = io.ReadAll(resp.Body)
				if err != nil {
					log.Println(err.Error())
					return err
				}
				log.Println("DSUB Broker Response")
				log.Println(string(rsp))
				subrsp := DSUBSubscribeResponse{}
				if err = xml.Unmarshal(rsp, &subrsp); err == nil {
					i.BrokerRef = subrsp.Body.SubscribeResponse.SubscriptionReference.Address
					if i.BrokerRef == "" {
						err = errors.New("no broker reference found in response message from dsub broker")
					} else {
						log.Printf("Successfully Created Subscription. Broker Ref :  %s", subrsp.Body.SubscribeResponse.SubscriptionReference.Address)
					}
				}
			}
		}
	}
	if err != nil {
		log.Println(err.Error())
	}
	return err
}
func NewSOAPRequest(url string, soapaction string, body string, timeout int64) (*http.Response, error) {
	var err error
	var req *http.Request
	var rsp *http.Response
	var duration time.Duration = time.Duration(timeout * int64(time.Millisecond))
	if req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(body)); err == nil {
		log.Printf("Set Request Body\n %s", body)
		req.Header.Set(cnst.SOAP_ACTION, soapaction)
		log.Printf("Set Request Header %s %s", cnst.SOAP_ACTION, soapaction)
		req.Header.Set(cnst.CONTENT_TYPE, cnst.SOAP_XML)
		log.Printf("Set Request Header %s %s", cnst.CONTENT_TYPE, cnst.SOAP_XML)
		req.Header.Set(cnst.ACCEPT, cnst.ALL)
		log.Printf("Set Request Header %s %s", cnst.ACCEPT, cnst.ALL)
		req.Header.Set(cnst.CONNECTION, cnst.KEEP_ALIVE)
		log.Printf("Set Request Header %s %s", cnst.CONNECTION, cnst.KEEP_ALIVE)
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()
		log.Printf("Sending Request to %s. Set Request Timeout %v ms", url, duration.Milliseconds())
		rsp, err = http.DefaultClient.Do(req.WithContext(ctx))
	}
	if err != nil {
		log.Println(err.Error())
	}
	if rsp == nil {
		log.Printf("No Response Received from %s", url)
	} else {
		log.Printf("Received Response - Status Code %v", rsp.StatusCode)
	}
	return rsp, err
}
func persistXDWDefinitions(xdwdefs map[string][]byte) error {
	cnt := 0
	for ref, def := range xdwdefs {
		if ref != "" {
			log.Println("Persisting XDW Definition for Pathway : " + ref)
			xdws := TUKXDWS{Action: cnst.INSERT}
			xdw := TUKXDW{
				Name:      ref,
				IsXDSMeta: false,
				XDW:       string(def),
			}
			xdws.XDW = append(xdws.XDW, xdw)
			if err := xdws.newEvent(); err == nil {
				log.Println("Persisted XDW Definition for Pathway : " + ref)
				cnt = cnt + 1
			} else {
				log.Println("Failed to Persist XDW Definition for Pathway : " + ref)
			}
		}
	}
	log.Printf("XDW's Persisted - %v", cnt)
	return nil
}
func (i *TUKServiceState) getFolderFiles() ([]fs.DirEntry, error) {
	input := i.BaseFolder + "/" + i.ConfigFolder

	var err error
	var f *os.File
	var fileInfo []fs.DirEntry
	f, err = os.Open(input)
	if err != nil {
		log.Println(err)
		return fileInfo, err
	}
	fileInfo, err = f.ReadDir(-1)
	f.Close()
	if err != nil {
		log.Println(err)
	}
	return fileInfo, err
}
func getXDWBrokerExpressions(xdwdef WorkflowDefinition) map[string]string {
	log.Printf("Parsing %s XDW Tasks for potential DSUB Broker Subscriptions", xdwdef.Ref)
	var brokerExps = make(map[string]string)
	for _, task := range xdwdef.Tasks {
		for _, inp := range task.Input {
			log.Printf("Checking Input Task %s", inp.Name)
			if strings.Contains(inp.Name, "^^") {
				brokerExps[inp.Name] = xdwdef.Ref
				log.Printf("Task %v %s task input %s included in potential DSUB Broker subscriptions", task.ID, task.Name, inp.Name)
			} else {
				log.Printf("Input Task %s does not require a dsub broker subscription", inp.Name)
			}
		}
		for _, out := range task.Output {
			log.Printf("Checking Output Task %s", out.Name)
			if strings.Contains(out.Name, "^^") {
				brokerExps[out.Name] = xdwdef.Ref
				log.Printf("Task %v %s task output %s included in potential DSUB Broker subscriptions", task.ID, task.Name, out.Name)
			} else {
				log.Printf("Output Task %s does not require a dsub broker subscription", out.Name)
			}
		}
	}
	return brokerExps
}
func (i *TUKXDWS) NewEvent() error {
	log.Printf("Sending Request to %s", TUK_DB_URL+cnst.TUK_DB_TABLE_XDWS)
	body, _ := json.Marshal(i)
	log.Println(string(body))
	bodyBytes, err := newTUKHTTPDBRequest(cnst.TUK_DB_TABLE_XDWS, body)
	if err == nil {
		if err := json.Unmarshal(bodyBytes, &i); err != nil {
			fmt.Println(err.Error())
		}
	}
	return err
}
func (i *Subscriptions) NewEvent() error {
	var err error
	var bodyBytes []byte

	log.Printf("Sending Request to %s", TUK_DB_URL+cnst.TUK_DB_TABLE_SUBSCRIPTIONS)
	for _, sub := range i.Subscriptions {
		tuksubs := Subscriptions{Action: cnst.INSERT}
		tuksubs.Subscriptions = append(tuksubs.Subscriptions, sub)
		body, _ := json.Marshal(tuksubs)
		bodyBytes, err = newTUKHTTPDBRequest(cnst.TUK_DB_TABLE_SUBSCRIPTIONS, body)
		successSubs := Subscriptions{}
		if err == nil {
			err = json.Unmarshal(bodyBytes, &successSubs)
			i.LastInsertId = successSubs.LastInsertId
		}
	}

	if err != nil {
		log.Println(err.Error())
	}
	return err
}
func newTUKHTTPDBRequest(resource string, body []byte) ([]byte, error) {
	var bodyBytes []byte
	var err error
	var req *http.Request
	var resp *http.Response
	client := &http.Client{}
	if req, err = http.NewRequest(http.MethodPost, TUK_DB_URL+resource, bytes.NewBuffer(body)); err == nil {
		req.Header.Add(cnst.CONTENT_TYPE, cnst.APPLICATION_JSON_CHARSET_UTF_8)
		if resp, err = client.Do(req); err == nil {
			log.Printf("Response Status Code %v\n", resp.StatusCode)
			if resp.StatusCode == 400 {
				return bodyBytes, errors.New("resp error code 400")
			}
			bodyBytes, err = io.ReadAll(resp.Body)
		}
	}
	if err != nil {
		log.Println(err.Error())
	}
	return bodyBytes, err
}
func newTemplateFuncMap() template.FuncMap {
	return template.FuncMap{
		"dtday":      utils.Tuk_Day(),
		"dtmonth":    utils.Tuk_Month,
		"dtyear":     utils.Tuk_Year,
		"mappedid":   getCSVal,
		"prettytime": utils.PrettyTime,
		"newUuid":    utils.NewUuid,
	}
}

func getCSVal(key string) string {
	val, ok := cs[key]
	if ok {
		return val
	}
	return key
}
