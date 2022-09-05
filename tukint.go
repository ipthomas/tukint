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
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
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
	Ready      int
	Open       int
	InProgress int
	Closed     int
	ServerURL  string
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
	EventId            int64  `json:"eventid"`
	Creationtime       string `json:"creationtime"`
	DocName            string `json:"docname"`
	ClassCode          string `json:"classcode"`
	ConfCode           string `json:"confcode"`
	FormatCode         string `json:"formatcode"`
	FacilityCode       string `json:"facilitycode"`
	PracticeCode       string `json:"practicecode"`
	Expression         string `json:"expression"`
	Authors            string `json:"authors"`
	XdsPid             string `json:"xdspid"`
	XdsDocEntryUid     string `json:"xdsdocentryuid"`
	RepositoryUniqueId string `json:"repositoryuniqueid"`
	NhsId              string `json:"nhsid"`
	User               string `json:"user"`
	Org                string `json:"org"`
	Role               string `json:"role"`
	Topic              string `json:"topic"`
	Pathway            string `json:"pathway"`
	Notes              string `json:"notes"`
	Version            string `json:"ver"`
	BrokerRef          string `json:"brokerref"`
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
	Input       []Input     `xml:"input,omitempty"`
	Output      []Output    `xml:"output,omitempty"`
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
	ServerURL    string `json:"serverurl"`
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
	Source  string
	Message string
}
type TukHttpServer struct {
	BaseFolder      string
	ConfigFolder    string
	TemplateFolder  string
	LogFolder       string
	LogToFile       bool
	CodeSystemFile  string
	BaseResourceUrl string
	Port            string
}

type TukAuthor struct {
	Person      string `json:"authorPerson"`
	Institution string `json:"authorInstitution"`
	Speciality  string `json:"authorSpeciality"`
	Role        string `json:"authorRole"`
}
type TukAuthors struct {
	Author []TukAuthor `json:"authors"`
}

var (
	hostname                           = ""
	port                               = ":8080"
	baseResourceUrl                    = "/eventservice"
	htmlTemplates                      *template.Template
	xmlTemplates                       *template.Template
	logFile                            *os.File
	base_Folder                        = ""
	log_Folder                         = base_Folder + "/logs"
	config_Folder                      = base_Folder + "/configs"
	templates_Folder                   = config_Folder + "/templates"
	codeSystem_File                    = config_Folder + "/codesystem.json"
	TUK_DB_URL                         = "https://5k2o64mwt5.execute-api.eu-west-1.amazonaws.com/beta/"
	DSUB_BROKER_URL                    = "http://spirit-test-01.tianispirit.co.uk:8081/SpiritXDSDsub/Dsub"
	DSUB_CONSUMER_URL                  = "https://cjrvrddgdh.execute-api.eu-west-1.amazonaws.com/beta/"
	PIX_MANAGER_URL                    = "http://spirit-test-01.tianispirit.co.uk:8081/SpiritPIXFhir/r4/Patient"
	REGIONAL_OID                       = "2.16.840.1.113883.2.1.3.31.2.1.1"
	NHS_OID                            = "2.16.840.1.113883.2.1.4.1"
	DSUB_ACK_TEMPLATE                  = "<SOAP-ENV:Envelope xmlns:SOAP-ENV='http://www.w3.org/2003/05/soap-envelope' xmlns:s='http://www.w3.org/2001/XMLSchema' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'><SOAP-ENV:Body/></SOAP-ENV:Envelope>"
	DSUB_SUBSCRIBE_TEMPLATE            = "{{define \"subscribe\"}}<SOAP-ENV:Envelope xmlns:SOAP-ENV='http://www.w3.org/2003/05/soap-envelope' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns:s='http://www.w3.org/2001/XMLSchema' xmlns:wsa='http://www.w3.org/2005/08/addressing'><SOAP-ENV:Header><wsa:Action SOAP-ENV:mustUnderstand='true'>http://docs.oasis-open.org/wsn/bw-2/NotificationProducer/SubscribeRequest</wsa:Action><wsa:MessageID>urn:uuid:{{newuuid}}</wsa:MessageID><wsa:ReplyTo SOAP-ENV:mustUnderstand='true'><wsa:Address>http://www.w3.org/2005/08/addressing/anonymous</wsa:Address></wsa:ReplyTo><wsa:To>{{.BrokerUrl}}</wsa:To></SOAP-ENV:Header><SOAP-ENV:Body><wsnt:Subscribe xmlns:wsnt='http://docs.oasis-open.org/wsn/b-2' xmlns:a='http://www.w3.org/2005/08/addressing' xmlns:rim='urn:oasis:names:tc:ebxml-regrep:xsd:rim:3.0' xmlns:wsa='http://www.w3.org/2005/08/addressing'><wsnt:ConsumerReference><wsa:Address>{{.ConsumerUrl}}</wsa:Address></wsnt:ConsumerReference><wsnt:Filter><wsnt:TopicExpression Dialect='http://docs.oasis-open.org/wsn/t-1/TopicExpression/Simple'>ihe:FullDocumentEntry</wsnt:TopicExpression><rim:AdhocQuery id='urn:uuid:742790e0-aba6-43d6-9f1f-e43ed9790b79'><rim:Slot name='{{.Topic}}'><rim:ValueList><rim:Value>('{{.Expression}}')</rim:Value></rim:ValueList></rim:Slot></rim:AdhocQuery></wsnt:Filter></wsnt:Subscribe></SOAP-ENV:Body></SOAP-ENV:Envelope>{{end}}"
	DSUB_CANCEL_TEMPLATE               = "{{define \"cancel\"}}<soap:Envelope xmlns:soap='http://www.w3.org/2003/05/soap-envelope'><soap:Header><Action xmlns='http://www.w3.org/2005/08/addressing' soap:mustUnderstand='true'>http://docs.oasis-open.org/wsn/bw-2/SubscriptionManager/UnsubscribeRequest</Action><MessageID xmlns='http://www.w3.org/2005/08/addressing' soap:mustUnderstand='true'>urn:uuid:{{.UUID}}</MessageID><To xmlns='http://www.w3.org/2005/08/addressing' soap:mustUnderstand='true'>{{.BrokerRef}}</To><ReplyTo xmlns='http://www.w3.org/2005/08/addressing' soap:mustUnderstand='true'><Address>http://www.w3.org/2005/08/addressing/anonymous</Address></ReplyTo></soap:Header><soap:Body><Unsubscribe xmlns='http://docs.oasis-open.org/wsn/b-2' xmlns:ns2='http://www.w3.org/2005/08/addressing' xmlns:ns3='http://docs.oasis-open.org/wsrf/bf-2' xmlns:ns4='urn:oasis:names:tc:ebxml-regrep:xsd:rim:3.0' xmlns:ns5='urn:oasis:names:tc:ebxml-regrep:xsd:rs:3.0' xmlns:ns6='urn:oasis:names:tc:ebxml-regrep:xsd:lcm:3.0' xmlns:ns7='http://docs.oasis-open.org/wsn/t-1' xmlns:ns8='http://docs.oasis-open.org/wsrf/r-2'/></soap:Body></soap:Envelope>{{end}}"
	SOAP_XML_Content_Type_EventHeaders = map[string]string{cnst.CONTENT_TYPE: cnst.SOAP_XML}
)

func Set_AWS_Env_Vars(dburl string, brokerurl string, pixurl string, nhsoid string, regoid string) {
	TUK_DB_URL = dburl
	DSUB_BROKER_URL = brokerurl
	PIX_MANAGER_URL = pixurl
	NHS_OID = nhsoid
	REGIONAL_OID = regoid
}
func SetTUKDBURL(dburl string) {
	TUK_DB_URL = dburl
}
func SetDSUBBrokerURL(brokerurl string) {
	DSUB_BROKER_URL = brokerurl
}
func SetDSUBConsumerURL(consumerurl string) {
	DSUB_CONSUMER_URL = consumerurl
}
func SetPIXURL(pixurl string) {
	PIX_MANAGER_URL = pixurl
}
func SetNHSOID(nhsoid string) {
	NHS_OID = nhsoid
}
func SetRegionalOID(regionaloid string) {
	REGIONAL_OID = regionaloid
}
func SetBaseFolder(baseFolder string) {
	base_Folder = baseFolder
}
func SetLogFolder(logFolder string) {
	log_Folder = base_Folder + "/" + logFolder
}
func SetConfigFolder(configFolder string) {
	config_Folder = base_Folder + "/" + configFolder
}
func SetTemplateFolder(templateFolder string) {
	templates_Folder = config_Folder + "/" + templateFolder
}

// SetCodeSystemFile sets the codesystem file.
//
//	The codesystem file can be used to provide mapping of values.
//	If the input value does not have a suffix of `.json`, .json is appended
//
// The default filename is `codesystem.json'
func SetCodeSystemFile(codeSystemFile string) {
	if strings.HasSuffix(codeSystemFile, ".json") {
		codeSystem_File = config_Folder + "/" + codeSystemFile
	} else {
		codeSystem_File = config_Folder + "/" + codeSystemFile + ".json"
	}
}

// SetServerPort sets the Tuk Server port.
//
//	The default is `8080'
func SetServerPort(srvport string) {
	if !strings.HasPrefix(srvport, ":") {
		port = ":" + srvport
	}
}

// SetServerResourceURL sets the Tuk Server URL base resource.
//
//	The default is `/eventservice'
func SetServerResourceURL(url string) {
	if !strings.HasPrefix(url, "/") {
		baseResourceUrl = "/" + url
	}
}

// SetFoldersAndFiles is a convieniance method to intialise the tuk interface system folders and files.
//
//	It calls :
//		SetBaseFolder(baseFolder)
//		SetLogFolder(logFolder)
//		SetConfigFolder(configFolder)
//		SetTemplateFolder(templateFolder)
//		SetCodeSystemFile(codeSysFile)
func SetFoldersAndFiles(baseFolder string, logFolder string, configFolder string, templateFolder string, codeSysFile string) {
	SetBaseFolder(baseFolder)
	SetLogFolder(logFolder)
	SetConfigFolder(configFolder)
	SetTemplateFolder(templateFolder)
	SetCodeSystemFile(codeSysFile)
}

func InitCodeSystem() {
	util.InitCodeSystem(codeSystem_File)
}

// InitLog sends log messages to a log file rather than the console
func InitLog() {
	var err error
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	mdir := log_Folder
	if _, err := os.Stat(mdir); errors.Is(err, fs.ErrNotExist) {
		if e2 := os.Mkdir(mdir, 0700); e2 != nil {
			log.Println(err.Error())
			return
		}
	}
	dir := mdir + "/" + util.Tuk_Year()
	if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		if e2 := os.Mkdir(dir, 0700); e2 != nil {
			log.Println(err.Error())
			return
		}
	}
	logFile, err = os.OpenFile(dir+"/"+util.Tuk_Month()+util.Tuk_Day()+".log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println(err.Error())
		return
	}
	log.Println("Using log file - " + logFile.Name())
	log.SetOutput(logFile)
	log.Println("-----------------------------------------------------------------------------------")
}

// CloseLog closes logging to the log file
func CloseLog() {
	logFile.Close()
}
func LoadTemplates() error {
	var err error
	htmlTemplates, err = template.New(cnst.HTML).Funcs(util.TemplateFuncMap()).ParseGlob(templates_Folder + "/*.html")
	if err != nil {
		return err
	}
	xmlTemplates, err = template.New(cnst.XML).Funcs(util.TemplateFuncMap()).ParseGlob(templates_Folder + "/*.xml")
	if err != nil {
		return err
	}
	log.Printf("Initialised %v HTML and %v XML templates", len(htmlTemplates.Templates()), len(xmlTemplates.Templates()))
	return nil
}
func InitLambdaVars() {
	if os.Getenv("TUK_DB_URL") != "" {
		TUK_DB_URL = os.Getenv("TUK_DB_URL")
		log.Printf("Set TUK_DB_URL %s from AWS environment variable", TUK_DB_URL)
	} else {
		log.Println("AWS TUK_DB_URL environment variable is empty")
	}
	if os.Getenv("PIX_MANAGER_URL") != "" {
		PIX_MANAGER_URL = os.Getenv("PIX_MANAGER_URL")
		log.Printf("Set PIX_MANAGER_URL %s from AWS environment variable", PIX_MANAGER_URL)
	} else {
		log.Println("AWS PIX_MANAGER_URL environment variable is empty")
	}
	if os.Getenv("DSUB_BROKER_URL") != "" {
		DSUB_BROKER_URL = os.Getenv("DSUB_BROKER_URL")
		log.Printf("Set DSUB_BROKER_URL %s from AWS environment variable", DSUB_BROKER_URL)
	} else {
		log.Println("AWS DSUB_BROKER_URL environment variable is empty")
	}
	if os.Getenv("REGIONAL_OID") != "" {
		REGIONAL_OID = os.Getenv("REGIONAL_OID")
		log.Printf("Set REGIONAL_OID %s from AWS environment variable", REGIONAL_OID)
	} else {
		log.Println("AWS REGIONAL_OID environment variable is empty")
	}
	if os.Getenv("NHS_OID") != "" {
		NHS_OID = os.Getenv("NHS_OID")
		log.Printf("Set NHS_OID %s from AWS environment variable", NHS_OID)
	} else {
		log.Println("AWS NHS_OID environment variable is empty")
	}
}
func (i *TukHttpServer) NewHTTPServer() {
	if i.BaseFolder != "" {
		SetBaseFolder(i.BaseFolder)
	}
	if base_Folder == "" {
		log.Println("Error - BaseFolder must be set")
		return
	}
	if i.LogFolder != "" {
		SetLogFolder(i.LogFolder)
	}
	if log_Folder == "" {
		SetLogFolder("logs")
	}
	if i.ConfigFolder != "" {
		SetConfigFolder(i.ConfigFolder)
	}
	if config_Folder == "" {
		SetConfigFolder("configs")
	}
	if i.TemplateFolder != "" {
		SetTemplateFolder(i.TemplateFolder)
	}
	if templates_Folder == "" {
		SetTemplateFolder("templates")
	}
	if i.CodeSystemFile != "" {
		SetCodeSystemFile(i.CodeSystemFile)
	}
	if codeSystem_File == "" {
		SetCodeSystemFile("codesystem")
	}
	if i.BaseResourceUrl != "" {
		SetServerResourceURL(i.BaseResourceUrl)
	}
	if i.Port != "" {
		SetServerPort(i.Port)
	}
	if err := LoadTemplates(); err != nil {
		log.Println(err.Error())
		return
	}
	hostname, _ = os.Hostname()
	http.HandleFunc(baseResourceUrl, writeResponseHeaders(route_TUK_Server_Request))
	log.Printf("Initialised HTTP Server - Listening on %s", GetServerURL())
	monitorApp()
	log.Fatal(http.ListenAndServe(port, nil))
}

func monitorApp() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		signalType := <-ch
		signal.Stop(ch)
		fmt.Println("")
		fmt.Println("***")
		fmt.Println("Exit command received. Exiting...")
		exitcode := 0
		switch signalType {
		case os.Interrupt:
			log.Println("FATAL: CTRL+C pressed")
		case syscall.SIGTERM:
			log.Println("FATAL: SIGTERM detected")
			exitcode = 1
		}
		os.Exit(exitcode)
	}()
}
func writeResponseHeaders(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Server", "Tiani_Spirit_UK")
		if r.Header.Get(cnst.ACCEPT) == cnst.APPLICATION_XML {
			w.Header().Set(cnst.CONTENT_TYPE, cnst.APPLICATION_XML)
		} else if r.Header.Get(cnst.ACCEPT) == cnst.APPLICATION_JSON {
			w.Header().Set(cnst.CONTENT_TYPE, cnst.APPLICATION_JSON)
		} else if r.Header.Get(cnst.ACCEPT) == cnst.ALL {
			w.Header().Set(cnst.CONTENT_TYPE, cnst.TEXT_HTML)
		} else {
			w.Header().Set(cnst.CONTENT_TYPE, cnst.TEXT_HTML)
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "accept, Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

		fn(w, r)
	}
}
func GetServerURL() string {
	return "http://" + hostname + port + baseResourceUrl
}
func InitXDWWorkflowDocument(wf Workflow) (XDWWorkflowDocument, error) {
	var err error
	xdwStruc := XDWWorkflowDocument{}
	err = json.Unmarshal([]byte(wf.XDW_Doc), &xdwStruc)
	return xdwStruc, err
}
func InitXDWDefinition(wf Workflow) (WorkflowDefinition, error) {
	var err error
	xdwdef := WorkflowDefinition{}
	err = json.Unmarshal([]byte(wf.XDW_Def), &xdwdef)
	return xdwdef, err
}
func route_TUK_Server_Request(rsp http.ResponseWriter, r *http.Request) {
	req := ClientRequest{ServerURL: GetServerURL()}
	if err := req.InitClientRequest(r); err == nil {
		req.Log()
		rsp.Write([]byte(req.ProcessClientRequest()))
	} else {
		log.Println(err.Error())
	}
}
func (i *ClientRequest) InitClientRequest(req *http.Request) error {
	log.Printf("Received http %s request", req.Method)
	req.ParseForm()
	i.Act = req.FormValue(cnst.ACT)
	i.User = req.FormValue("user")
	i.Org = req.FormValue("org")
	i.Orgoid = util.GetCodeSystemVal(req.FormValue("org"))
	i.Role = req.FormValue("role")
	i.NHS = req.FormValue("nhs")
	i.PID = req.FormValue("pid")
	i.PIDOrg = req.FormValue("pidorg")
	i.PIDOID = util.GetCodeSystemVal(req.FormValue("pidorg"))
	i.FamilyName = req.FormValue("familyname")
	i.GivenName = req.FormValue("givenname")
	i.DOB = req.FormValue("dob")
	i.Gender = req.FormValue("gender")
	i.ZIP = req.FormValue("zip")
	i.Status = req.FormValue("status")
	i.ID = util.GetIntFromString(req.FormValue("id"))
	i.Task = req.FormValue(cnst.TASK)
	i.Pathway = req.FormValue(cnst.PATHWAY)
	i.Version = util.GetIntFromString(req.FormValue("version"))
	i.XDWKey = req.FormValue("xdwkey")
	i.ReturnFormat = req.Header.Get(cnst.ACCEPT)
	return nil
}
func (req *ClientRequest) ProcessClientRequest() string {
	log.Printf("Processing %s Request", req.Act)
	switch req.Act {
	case cnst.DASHBOARD:
		return req.NewDashboardRequest()
	case cnst.WORKFLOWS:
		return req.NewWorkflowsRequest()
	case cnst.WORKFLOW:
		return req.NewWorkflowRequest()
	case cnst.TASK:
		return req.NewTaskRequest()
	case cnst.PATIENT:
		return req.NewPatientRequest()
	}
	return "Nothing to process"
}
func (i *ClientRequest) NewPatientRequest() string {
	pat, err := NewPIXmConsumer(i.NHS, NHS_OID)
	if err != nil {
		log.Println(err.Error())
		return err.Error()
	}
	var b bytes.Buffer
	err = htmlTemplates.ExecuteTemplate(&b, "pixpatient", pat)
	if err != nil {
		log.Println(err.Error())
	}
	return b.String()
}
func (i *ClientRequest) NewTaskRequest() string {
	if i.ID < 1 || i.Pathway == "" {
		return "Invalid request. Task ID and Pathway required"
	}
	wfdoc := XDWWorkflowDocument{}
	wfdef := WorkflowDefinition{}

	wfs := Workflows{Action: cnst.SELECT}
	wf := Workflow{XDW_Key: i.Pathway, Version: i.Version}
	wfs.Workflows = append(wfs.Workflows, wf)
	if err := wfs.NewTukDBEvent(); err != nil {
		log.Println(err.Error())
		return err.Error()
	}
	if wfs.Count != 1 {
		return "No Workflow found for " + i.Pathway + " version " + util.GetStringFromInt(i.Version)
	}
	if err := json.Unmarshal([]byte(wfs.Workflows[1].XDW_Doc), &wfdoc); err != nil {
		log.Println(err.Error())
		return err.Error()
	}
	if err := json.Unmarshal([]byte(wfs.Workflows[1].XDW_Def), &wfdef); err != nil {
		log.Println(err.Error())
		return err.Error()
	}
	type itmplt struct {
		ServerURL string
		TaskId    string
		XDW       XDWWorkflowDocument
		XDWDef    WorkflowDefinition
	}
	it := itmplt{TaskId: util.GetStringFromInt(i.ID), ServerURL: GetServerURL(), XDW: wfdoc, XDWDef: wfdef}
	var b bytes.Buffer
	err := htmlTemplates.ExecuteTemplate(&b, "snip_workflow_task", it)
	if err != nil {
		log.Println(err.Error())
	}
	return b.String()
}
func (i *ClientRequest) NewWorkflowsRequest() string {
	type TmpltWorkflow struct {
		Created   string
		NHS       string
		Pathway   string
		XDWKey    string
		Published bool
		Version   int
		XDW       XDWWorkflowDocument
		XDWDef    WorkflowDefinition
		Patient   PIXPatient
	}
	type TmpltWorkflows struct {
		Count     int
		ServerURL string
		Workflows []TmpltWorkflow
	}
	tmpltwfs := TmpltWorkflows{ServerURL: i.ServerURL}

	tukwfs := Workflows{Action: cnst.SELECT}
	if err := tukwfs.NewTukDBEvent(); err != nil {
		log.Println(err.Error())
		return err.Error()
	}
	log.Printf("Processing %v workflows", tukwfs.Count)
	for _, wf := range tukwfs.Workflows {
		if wf.Id > 0 {
			pat := PIXPatient{}
			log.Printf("Initialising workflow document - id %v", wf.Id)
			xdw, err := InitXDWWorkflowDocument(wf)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			log.Printf("Initialised Workflow document - %s", wf.XDW_Key)
			xdwdef, err := InitXDWDefinition(wf)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			log.Printf("Initialised Workflow definition for Workflow document %s", xdwdef.Ref)
			pat, _ = NewPIXmConsumer(xdw.Patient.ID.Extension, NHS_OID)
			if len(pat.NHSID) != 10 {
				log.Println("Unable to obtain valid patient details")
			} else {
				log.Printf("Obtained Patient details for Workflow %s", wf.XDW_Key)
			}
			tmpltworkflow := TmpltWorkflow{}
			if i.Status != "" {
				log.Printf("Obtaining Workflows with status = %s", i.Status)
				if strings.EqualFold(xdw.WorkflowStatus, i.Status) {
					tmpltworkflow.Created = wf.Created
					tmpltworkflow.Published = wf.Published
					tmpltworkflow.Version = wf.Version
					tmpltworkflow.XDWKey = wf.XDW_Key
					tmpltworkflow.Pathway, tmpltworkflow.NHS = util.SplitXDWKey(tmpltworkflow.XDWKey)
					tmpltworkflow.XDW = xdw
					tmpltworkflow.XDWDef = xdwdef
					tmpltworkflow.Patient = pat
					tmpltwfs.Workflows = append(tmpltwfs.Workflows, tmpltworkflow)
					tmpltwfs.Count = tmpltwfs.Count + 1
					log.Printf("Including Workflow %s - Status %s", wf.XDW_Key, xdw.WorkflowStatus)
				}
			} else {
				tmpltworkflow.Created = wf.Created
				tmpltworkflow.Published = wf.Published
				tmpltworkflow.Version = wf.Version
				tmpltworkflow.XDWKey = wf.XDW_Key
				tmpltworkflow.Pathway, tmpltworkflow.NHS = util.SplitXDWKey(tmpltworkflow.XDWKey)
				tmpltworkflow.XDW = xdw
				tmpltworkflow.XDWDef = xdwdef
				tmpltworkflow.Patient = pat
				tmpltwfs.Workflows = append(tmpltwfs.Workflows, tmpltworkflow)
				tmpltwfs.Count = tmpltwfs.Count + 1
				log.Printf("Including Workflow %s - Status %s", wf.XDW_Key, xdw.WorkflowStatus)
			}
		}
	}
	var b bytes.Buffer
	err := htmlTemplates.ExecuteTemplate(&b, cnst.WORKFLOWS, tmpltwfs)
	if err != nil {
		log.Println(err.Error())
	}
	log.Printf("Returning %v Workflows", tmpltwfs.Count)
	return b.String()
}
func (i *ClientRequest) NewWorkflowRequest() string {
	if i.XDWKey == "" && (i.Pathway == "" && i.NHS == "") {
		return "Invalid request. Either xdwkey or Both Pathway and NHS ID are required"
	}
	if i.XDWKey == "" {
		i.XDWKey = strings.ToUpper(i.Pathway) + i.NHS
	}
	xdw := XDWWorkflowDocument{}
	wfs := Workflows{Action: cnst.SELECT}
	wf := Workflow{XDW_Key: i.XDWKey, Version: i.Version}

	wfs.Workflows = append(wfs.Workflows, wf)
	wfs.NewTukDBEvent()

	if wfs.Count != 1 {
		return "No Workflow Found with XDW Key - " + i.XDWKey
	}
	if err := json.Unmarshal([]byte(wfs.Workflows[1].XDW_Doc), &xdw); err != nil {
		log.Println(err.Error())
		return err.Error()
	}
	type wftmplt struct {
		ServerURL string
		XDW       XDWWorkflowDocument
	}
	itmplt := wftmplt{ServerURL: GetServerURL(), XDW: xdw}
	var b bytes.Buffer
	err := htmlTemplates.ExecuteTemplate(&b, cnst.WORKFLOW, itmplt)
	if err != nil {
		log.Println(err.Error())
	}
	log.Printf("Returning %v Workflow", xdw.WorkflowDefinitionReference)
	return b.String()
}
func (i *ClientRequest) NewDashboardRequest() string {
	dashboard := Dashboard{}
	wfs := Workflows{Action: cnst.SELECT}
	wfs.NewTukDBEvent()
	log.Printf("Processing %v workflows", wfs.Count)
	for _, wf := range wfs.Workflows {
		dashboard.Total = dashboard.Total + 1
		xdw, err := InitXDWWorkflowDocument(wf)
		if err != nil {
			continue
		}
		json.Unmarshal([]byte(wf.XDW_Doc), &xdw)
		if wf.Version == 0 {
			log.Printf("Workflow Created on %s for Patient NHS ID %s Workflow Status %s", xdw.EffectiveTime.Value, xdw.Patient.ID.Extension, xdw.WorkflowStatus)
			switch xdw.WorkflowStatus {
			case "READY":
				dashboard.Ready = dashboard.Ready + 1
			case "OPEN":
				dashboard.Open = dashboard.Open + 1
			case "IN_PROGRESS":
				dashboard.InProgress = dashboard.InProgress + 1
			case "COMPLETE":
				dashboard.Closed = dashboard.Closed + 1
			}
		}
	}

	var b bytes.Buffer
	err := htmlTemplates.ExecuteTemplate(&b, "dashboardwidget", dashboard)
	if err != nil {
		log.Println(err.Error())
	}
	return b.String()
}
func (i *EventMessage) NewDSUBBrokerEvent() error {
	var err error
	var dsubNotify DSUBNotifyMessage
	InitLambdaVars()
	log.Printf("Processing DSUB Broker Notfy Message\n%s", i.Message)
	if dsubNotify, err = i.NewDSUBNotifyMessage(); err == nil {
		dsubEvent := Event{}
		dsubEvent.InitDSUBEvent(dsubNotify)
		if dsubEvent.BrokerRef == "" {
			return errors.New("no subscription ref found in notification message")
		}
		log.Printf("Found Subscription Reference %s. Setting Event state from Notify Message", dsubEvent.BrokerRef)
		if dsubEvent.XdsPid == "" {
			return errors.New("no pid found in notification message")
		}
		log.Printf("Checking for TUK Event subscriptions with Broker Ref = %s", dsubEvent.BrokerRef)
		tukdbSubs := Subscriptions{Action: "select"}
		tukdbSub := Subscription{BrokerRef: dsubEvent.BrokerRef}
		tukdbSubs.Subscriptions = append(tukdbSubs.Subscriptions, tukdbSub)
		if err = tukdbSubs.NewTukDBEvent(); err == nil {
			log.Printf("TUK Event Subscriptions Count : %v", tukdbSubs.Count)
			if tukdbSubs.Count > 0 {
				log.Printf("Obtaining NHS ID. Using %s", dsubEvent.XdsPid+":"+REGIONAL_OID)
				pat := PIXPatient{}
				if pat, err = NewPIXmConsumer(dsubEvent.XdsPid, REGIONAL_OID); err != nil {
					log.Println(err.Error())
					return err
				}
				if len(pat.NHSID) != 10 {
					log.Printf("No Valid NHS ID returned in pix query for PID %s", dsubEvent.XdsPid)
					return errors.New("no valid nhs id returned in pix query for pid " + dsubEvent.XdsPid)
				}
				for _, dbsub := range tukdbSubs.Subscriptions {
					log.Printf("Found %s %s Subsription for Broker Ref %s", dbsub.Pathway, dbsub.Expression, dbsub.BrokerRef)
					tukevs := Events{Action: "insert"}
					dsubEvent.Pathway = dbsub.Pathway
					dsubEvent.Topic = dbsub.Topic
					dsubEvent.NhsId = pat.NHSID
					tukevs.Events = append(tukevs.Events, dsubEvent)
					if err = tukevs.NewTukDBEvent(); err == nil {
						log.Printf("Created TUK Event for Pathway %s Expression %s Broker Ref %s", dsubEvent.Pathway, dsubEvent.Expression, dsubEvent.BrokerRef)
						dsubEvent.UpdateWorkflow(pat)
					}
				}
			} else {
				log.Printf("No Subscriptions found with brokerref = %s. Sending Cancel request to Broker", dsubEvent.BrokerRef)
				dsubCancel := DSUBCancel{BrokerRef: dsubEvent.BrokerRef, UUID: util.NewUuid()}
				dsubCancel.NewEvent()
			}
		}
	}
	return nil
}
func (i *EventMessage) NewDSUBNotifyMessage() (DSUBNotifyMessage, error) {
	dsubNotify := DSUBNotifyMessage{}
	if i.Message == "" {
		return dsubNotify, errors.New("message is empty")
	}
	log.Println(i.Message)
	notifyElement := util.GetXMLNodeList(i.Message, cnst.DSUB_NOTIFY_ELEMENT)
	if notifyElement == "" {
		return dsubNotify, errors.New("unable to locate notify element in received message")
	}
	if err := xml.Unmarshal([]byte(notifyElement), &dsubNotify); err != nil {
		return dsubNotify, err
	}
	return dsubNotify, nil
}
func (i *Event) UpdateWorkflow(pat PIXPatient) {
	log.Printf("Updating %s Workflow for patient %s %s %s", i.Pathway, pat.GivenName, pat.FamilyName, i.NhsId)
	tukwfdefs := XDWS{Action: cnst.SELECT}
	tukwfdef := XDW{Name: strings.ToUpper(i.Pathway)}
	tukwfdefs.XDW = append(tukwfdefs.XDW, tukwfdef)
	if err := tukwfdefs.NewTukDBEvent(); err != nil {
		log.Println(err.Error())
		return
	}
	if tukwfdefs.Count == 1 {
		log.Println("Found Workflow Definition for Pathway " + i.Pathway)
		wfdef := WorkflowDefinition{}
		if err := json.Unmarshal([]byte(tukwfdefs.XDW[1].XDW), &wfdef); err != nil {
			log.Println(err.Error())
			return
		}
		log.Println("Parsed Workflow Definition for Pathway " + wfdef.Ref)

		log.Printf("Searching for exisitng workflow for %s %s", strings.ToUpper(i.Pathway), i.NhsId)
		tukwfdocs := Workflows{Action: cnst.SELECT}
		tukwfdoc := Workflow{XDW_Key: strings.ToUpper(i.Pathway) + i.NhsId}
		tukwfdocs.Workflows = append(tukwfdocs.Workflows, tukwfdoc)
		if err := tukwfdocs.NewTukDBEvent(); err != nil {
			log.Println(err.Error())
			return
		}
		activeWorkflow := XDWWorkflowDocument{}
		if tukwfdocs.Count == 0 {
			log.Printf("No existing workflow state found for %s %s", strings.ToUpper(i.Pathway), i.NhsId)
			activeWorkflow := i.NewXDWContentCreator(wfdef, pat)
			log.Println("Creating Workflow state")
			var wfdocbytes []byte
			var wfdefbytes []byte
			var err error
			if wfdocbytes, err = json.Marshal(activeWorkflow); err != nil {
				log.Println(err.Error())
				return
			}
			if wfdefbytes, err = json.Marshal(wfdef); err != nil {
				log.Println(err.Error())
				return
			}
			newwfdocs := Workflows{Action: cnst.INSERT}
			newwfdoc := Workflow{
				XDW_Key:   strings.ToUpper(i.Pathway) + i.NhsId,
				XDW_UID:   activeWorkflow.ID.Extension,
				XDW_Doc:   string(wfdocbytes),
				XDW_Def:   string(wfdefbytes),
				Version:   0,
				Published: false,
			}
			newwfdocs.Workflows = append(newwfdocs.Workflows, newwfdoc)
			if err := newwfdocs.NewTukDBEvent(); err != nil {
				log.Println(err.Error())
				return
			}
			log.Println("Created new Workflow Document")
		} else {
			log.Printf("Existing Workflow document found for Pathway %s NHS ID %s", i.Pathway, i.NhsId)
			if err := json.Unmarshal([]byte(tukwfdocs.Workflows[1].XDW_Doc), &activeWorkflow); err != nil {
				log.Println(err.Error())
			}
		}
		NewXDWContentUpdator(activeWorkflow, pat)
	} else {
		log.Printf("Warning. No XDW Definition found for pathway %s", i.Pathway)

	}
}
func NewXDWContentUpdator(wf XDWWorkflowDocument, pat PIXPatient) {
	log.Printf("Updating %s Workflow for NHS ID %s with latest events", wf.WorkflowDefinitionReference, pat.NHSID)

}

// func (i *Event) updateActiveWorkflow() error {
// 	log.Println("Updating Active Workflow")
// 	if i.XDWWorkflowDocument.WorkflowStatus != "COMPLETE" {
// 		log.Println("Workflow is not complete. Updating Workflow Tasks")
// 		tukEvents := Events{Action: "select"}
// 		tukEvent := Event{Pathway: i.Pathway, NhsId: i.NhsId}
// 		tukEvents.Events = append(tukEvents.Events, tukEvent)
// 		if err := tukEvents.NewEvent(); err != nil {
// 			log.Println(err.Error())
// 			return err
// 		}
// 		i.Events = tukEvents
// 		sort.Sort(eventsList(i.Events.Events))
// 		log.Printf("Updating %s Workflow Tasks with %v Events", i.XDWWorkflowDocument.WorkflowDefinitionReference, len(i.Events.Events))
// 		log.Println("Replacing Active Workflow State with Updated Workflow State")
// 	}
// 	return nil
// }

//	func (i *Event) updateWorkflowTasks() error {
//		tukEvents := Events{Action: "select"}
//		tukEvent := Event{Pathway: i.Pathway, NhsId: i.NhsId}
//		tukEvents.Events = append(tukEvents.Events, tukEvent)
//		if err := tukEvents.NewEvent(); err != nil {
//			return err
//		}
//		i.Events = tukEvents
//		sort.Sort(eventsList(i.Events.Events))
//		log.Printf("Updating %s Workflow Tasks with %v Events", i.XDWWorkflowDocument.WorkflowDefinitionReference, len(i.Events.Events))
//		var newVers = false
//		for _, ev := range i.Events.Events {
//			for k, wfdoctask := range i.XDWWorkflowDocument.TaskList.XDWTask {
//				log.Println("Checking Workflow Document Task " + wfdoctask.TaskData.TaskDetails.Name + " for matching Events")
//				for inp, input := range wfdoctask.TaskData.Input {
//					if ev.Expression == input.Part.Name {
//						log.Println("Matched workflow document task " + wfdoctask.TaskData.TaskDetails.ID + " Input Part : " + input.Part.Name + " with Event Expression : " + ev.Expression + " Status : " + wfdoctask.TaskData.TaskDetails.Status)
//						if !i.isInputRegistered(k, ev) {
//							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.LastModifiedTime = time.Now().Format(time.RFC3339)
//							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.AttachedTime = time.Now().Format(time.RFC3339)
//							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.AttachedBy = ev.User + " " + ev.Org + " " + ev.Role
//							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.Status = "REQUESTED"
//							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.ActualOwner = ev.User + " " + ev.Org + " " + ev.Role
//							if strings.HasSuffix(wfdoctask.TaskData.Input[inp].Part.AttachmentInfo.AccessType, "XDSregistered") {
//								i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.Identifier = ev.RepositoryUniqueId + ":" + ev.XdsDocEntryUid
//								i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.HomeCommunityId, _ = tukdb.GetLocalId(constants.XDSDOMAIN)
//								i.newTaskEvent(k, strconv.Itoa(ev.Id), ev.CreationTime, ev.Expression)
//							} else {
//								i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.Identifier = strconv.Itoa(ev.Id)
//								i.newTaskEvent(k, strconv.Itoa(ev.Id), ev.CreationTime, ev.Expression)
//							}
//							i.XDWWorkflowDocument.WorkflowStatus = "IN_PROGRESS"
//						}
//					}
//				}
//				for oup, output := range i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output {
//					if ev.Expression == output.Part.Name {
//						log.Println("Matched workflow document task " + wfdoctask.TaskData.TaskDetails.ID + " Output Part : " + output.Part.Name + " with Event Expression : " + ev.Expression + " Status : " + wfdoctask.TaskData.TaskDetails.Status)
//						if !i.isOutputRegistered(k, ev) {
//							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.LastModifiedTime = time.Now().Format(time.RFC3339)
//							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.AttachedTime = time.Now().Format(time.RFC3339)
//							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.AttachedBy = ev.User + " " + ev.Org + " " + ev.Role
//							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.ActualOwner = ev.User + " " + ev.Org + " " + ev.Role
//							i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.Status = "IN_PROGRESS"
//							var tid = Newid()
//							if strings.HasSuffix(wfdoctask.TaskData.Output[oup].Part.AttachmentInfo.AccessType, "XDSregistered") {
//								i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.Identifier = ev.RepositoryUniqueId + ":" + ev.XdsDocEntryUid
//								i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.HomeCommunityId, _ = tukdb.GetLocalId(constants.XDSDOMAIN)
//								tid, newVers = i.newTaskEvent(k, strconv.Itoa(ev.Id), time.Now().Format(time.RFC3339), ev.Expression)
//								if newVers {
//									wfseqnum, _ := strconv.ParseInt(i.XDWWorkflowDocument.WorkflowDocumentSequenceNumber, 0, 0)
//									wfseqnum = wfseqnum + 1
//									i.XDWWorkflowDocument.WorkflowDocumentSequenceNumber = strconv.Itoa(int(wfseqnum))
//									i.newDocEvent(ev, tid, k)
//								}
//							} else {
//								i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.Identifier = strconv.Itoa(ev.Id)
//								tid, newVers = i.newTaskEvent(k, strconv.Itoa(ev.Id), time.Now().Format(time.RFC3339), ev.Expression)
//								if newVers {
//									wfseqnum, _ := strconv.ParseInt(i.XDWWorkflowDocument.WorkflowDocumentSequenceNumber, 0, 0)
//									wfseqnum = wfseqnum + 1
//									i.XDWWorkflowDocument.WorkflowDocumentSequenceNumber = strconv.Itoa(int(wfseqnum))
//									i.newDocEvent(ev, tid, k)
//								}
//							}
//							i.XDWWorkflowDocument.WorkflowStatus = "IN_PROGRESS"
//						}
//					}
//				}
//			}
//		}
//		for task := range i.XDWWorkflowDocument.TaskList.XDWTask {
//			if i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskData.TaskDetails.Status != "COMPLETE" {
//				if i.isTaskCompleteBehaviorMet(task) {
//					i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskData.TaskDetails.Status = "COMPLETE"
//				}
//			}
//		}
//		for task := range i.XDWWorkflowDocument.TaskList.XDWTask {
//			if i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskData.TaskDetails.Status != "COMPLETE" {
//				if i.isTaskCompleteBehaviorMet(task) {
//					i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskData.TaskDetails.Status = "COMPLETE"
//				}
//			}
//		}
//		if isWorkflowCompleteBehaviorMet(i) {
//			i.XDWWorkflowDocument.WorkflowStatus = "COMPLETE"
//			tevidstr := strconv.Itoa(int(i.newODDEvent("WORKFLOW", "CLOSE", "All Workflow Completion Behaviour Conditions Met. Workflow Closed")))
//			docevent := DocumentEvent{}
//			docevent.Author = i.User
//			docevent.TaskEventIdentifier = tevidstr
//			docevent.EventTime = i.Creationtime
//			docevent.EventType = "CLOSE"
//			docevent.PreviousStatus = i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent[len(i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent)-1].ActualStatus
//			docevent.ActualStatus = "COMPLETE"
//			i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent = append(i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent, docevent)
//				for k := range i.XDWWorkflowDocument.TaskList.XDWTask {
//					i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.TaskDetails.Status = "COMPLETE"
//				}
//				log.Println("Closed Workflow. Total Workflow Document Events " + strconv.Itoa(len(i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent)))
//			}
//			return nil
//		}
//
//	func (i *Event) isInputRegistered(ev Event, k int) bool {
//		for _, input := range i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Input {
//			if (ev.Expression == input.Part.Name) && (input.Part.AttachmentInfo.AttachedBy == i.User+" "+i.Org) {
//				log.Println("Event is already registered. Skipping Event ")
//				return true
//			}
//		}
//		log.Println("Processing New Event ")
//		return false
//	}
//
//	func (i *Event) isOutputRegistered(k int) bool {
//		for _, output := range i.XDWWorkflowDocument.TaskList.XDWTask[k].TaskData.Output {
//			if (i.Expression == output.Part.Name) && (output.Part.AttachmentInfo.AttachedBy == i.User+" "+i.Org) {
//				log.Println("Event is already registered. Skipping Event ")
//				return true
//			}
//		}
//		log.Println("Processing New Event ")
//		return false
//	}
//
//	func (i *Event) newDocEvent(tid string, k int) {
//		docevent := DocumentEvent{}
//		docevent.Author = i.User
//		docevent.TaskEventIdentifier = tid
//		docevent.EventTime = time.Now().Format(time.RFC3339)
//		docevent.EventType = i.Expression
//		docevent.PreviousStatus = i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent[len(i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent)-1].ActualStatus
//		docevent.ActualStatus = "IN_PROGRESS"
//		i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent = append(i.XDWWorkflowDocument.WorkflowStatusHistory.DocumentEvent, docevent)
//	}
//
//	func (i *Event) newTaskEvent(task int, evid string, evtime string, evtype string) (string, bool) {
//			for _, tev := range i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskEventHistory.TaskEvent {
//				if tev.ID == evid {
//					log.Println("Task Event Exists")
//					return tev.ID, false
//				}
//			}
//			tid64, _ := strconv.ParseInt(evid, 0, 0)
//			nextTaskEventId := strconv.Itoa(int(tid64))
//			nte := TaskEvent{
//				ID:         evid,
//				EventTime:  evtime,
//				Identifier: evid,
//				EventType:  evtype,
//				Status:     "COMPLETE",
//			}
//			i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskEventHistory.TaskEvent = append(i.XDWWorkflowDocument.TaskList.XDWTask[task].TaskEventHistory.TaskEvent, nte)
//			return nextTaskEventId, true
//		}

// NewPIXmConsumer takes a patient id and oid as string input and creates a PIXm Cosnumer actor to perform a PIXm query against an IHE PIXm compliant server
// if a unique patient is returned in the query response a PIXPatient struct is returned containing the patient demographics and ID's
func NewPIXmConsumer(pid string, pidoid string) (PIXPatient, error) {
	var err error
	pat := PIXPatient{}
	pixmQuery := PIXmQuery{PID: pid, PIDOID: pidoid}
	if err = pixmQuery.InitPIXPatient(); err == nil {
		if pixmQuery.Count != 1 {
			err = errors.New("no unique patient returned")
		} else {
			pat = pixmQuery.Response[0]
		}
	}
	if err != nil {
		log.Println(err.Error())
	}
	return pat, err
}

// New XDWDefinition takes an input string containing the workflow ref. It returns a WorkflowDefinition struc for the requested workflow
func NewXDWDefinition(workflow string) (WorkflowDefinition, error) {
	var err error
	xdwdef := WorkflowDefinition{}
	xdws := XDWS{Action: cnst.SELECT}
	xdw := XDW{Name: workflow}
	xdws.XDW = append(xdws.XDW, xdw)
	err = xdws.NewTukDBEvent()
	if xdws.Count != 1 {
		err = errors.New("no xdw definition found for workflow " + workflow)
	} else {
		json.Unmarshal([]byte(xdws.XDW[1].XDW), &xdwdef)
	}
	if err != nil {
		log.Println(err.Error())
	}
	return xdwdef, err
}

// NewXDWContentCreator takes input string for author details, a workflo definition and patient struct. It returns a new XDW compliant Document
func NewXDWContentCreator(author string, authorPrefix string, authorOrg string, authorOID string, xdwdef WorkflowDefinition, pat PIXPatient) XDWWorkflowDocument {
	log.Printf("Creating New %s XDW Document for NHS ID %s", xdwdef.Ref, pat.NHSID)
	xdwdoc := XDWWorkflowDocument{}
	var authorname = author
	var authoroid = authorOID
	var wfid = util.Newid()
	xdwdoc.Xdw = cnst.XDWNameSpace
	xdwdoc.Hl7 = cnst.HL7NameSpace
	xdwdoc.WsHt = cnst.WHTNameSpace
	xdwdoc.Xsi = cnst.XMLNS_XSI
	xdwdoc.XMLName.Local = cnst.XDWNameLocal
	xdwdoc.SchemaLocation = cnst.WorkflowDocumentSchemaLocation
	xdwdoc.ID.Root = strings.ReplaceAll(cnst.WorkflowInstanceId, "^", "")
	xdwdoc.ID.Extension = wfid
	xdwdoc.ID.AssigningAuthorityName = "ICS"
	xdwdoc.EffectiveTime.Value = util.Tuk_Time()
	xdwdoc.ConfidentialityCode.Code = xdwdef.Confidentialitycode
	xdwdoc.Patient.ID.Root = pat.NHSOID
	xdwdoc.Patient.ID.Extension = pat.NHSID
	xdwdoc.Patient.ID.AssigningAuthorityName = authorOrg
	xdwdoc.Author.AssignedAuthor.ID.Root = authoroid
	xdwdoc.Author.AssignedAuthor.ID.Extension = strings.ToUpper(authorname)
	xdwdoc.Author.AssignedAuthor.ID.AssigningAuthorityName = strings.ToUpper(authorname)
	xdwdoc.Author.AssignedAuthor.AssignedPerson.Name.Family = author
	xdwdoc.Author.AssignedAuthor.AssignedPerson.Name.Prefix = authorPrefix
	xdwdoc.WorkflowInstanceId = wfid + cnst.WorkflowInstanceId
	xdwdoc.WorkflowDocumentSequenceNumber = "1"
	xdwdoc.WorkflowStatus = "READY"
	xdwdoc.WorkflowDefinitionReference = strings.ToUpper(xdwdef.Ref) + pat.NHSID

	for _, t := range xdwdef.Tasks {
		task := XDWTask{}
		task.TaskData.TaskDetails.ID = t.ID
		task.TaskData.TaskDetails.TaskType = t.Tasktype
		task.TaskData.TaskDetails.Name = t.Name
		task.TaskData.TaskDetails.ActualOwner = t.Owner
		task.TaskData.TaskDetails.CreatedBy = author
		task.TaskData.TaskDetails.CreatedTime = xdwdoc.EffectiveTime.Value
		task.TaskData.TaskDetails.RenderingMethodExists = "false"
		task.TaskData.TaskDetails.LastModifiedTime = task.TaskData.TaskDetails.CreatedTime
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
		tev.EventTime = task.TaskData.TaskDetails.LastModifiedTime
		tev.ID = t.ID
		tev.Identifier = t.ID + "00"
		tev.EventType = "Create_Task"
		tev.Status = "COMPLETE"
		task.TaskEventHistory.TaskEvent = append(task.TaskEventHistory.TaskEvent, tev)
		xdwdoc.TaskList.XDWTask = append(xdwdoc.TaskList.XDWTask, task)
		log.Printf("Created Workflow Task %s Event Identifier %s", tev.ID, tev.Identifier)
	}
	docevent := DocumentEvent{}
	docevent.Author = author + " - " + authorPrefix + " - " + authorOrg
	docevent.TaskEventIdentifier = "100"
	docevent.EventTime = xdwdoc.EffectiveTime.Value
	docevent.EventType = "Create_Workflow"
	docevent.PreviousStatus = ""
	docevent.ActualStatus = "READY"
	log.Println("Created Workflow Document Event - Set status to 'READY'")
	xdwdoc.WorkflowStatusHistory.DocumentEvent = append(xdwdoc.WorkflowStatusHistory.DocumentEvent, docevent)

	log.Println("Created new " + xdwdoc.WorkflowDefinitionReference + " Workflow for Patient " + pat.NHSID)
	return xdwdoc
}

// RegisterXDWDefinitions loads and parses xdw definition files (with suffix `_xdwdef.json“) in the config folder.
// Any exisitng xdw definition for the workflow is deleted along with any tuk event subscriptions associated with the workflow
// DSUB Broker Subscriptions are then created for the workflow tasks.
// For each successful broker subcription, a Tuk Event subscription with the broker ref, workflow, topic and expression is created
// The new xdw definition is then persisted
// It returns a json string response containing the subscriptions created for the workflow
//
// ** NOTE ** Before calling RegisterXDWDefinitions() ensure all environment vars are set. For example:-
//
//		tukint.SetFoldersAndFiles(basepath, "logs", "configs", "templates", "codesystem")
//		tukint.SetTUKDBURL("https://5k2o64mwt5.execute-api.eu-west-1.amazonaws.com/beta/")
//		tukint.SetDSUBBrokerURL("http://spirit-test-01.tianispirit.co.uk:8081/SpiritXDSDsub/Dsub")
//		tukint.SetDSUBConsumerURL("https://cjrvrddgdh.execute-api.eu-west-1.amazonaws.com/beta/")
//
//	If you want the log output sent to a file rather than the terminal/console call tukint.InitLog() before calling RegisterXDWDefinitions() and tukint.CloseLog() before exiting
func RegisterXDWDefinitions() (Subscriptions, error) {
	var folderfiles []fs.DirEntry
	var file fs.DirEntry
	var err error
	var rspSubs = Subscriptions{Action: cnst.INSERT}
	if folderfiles, err = util.GetFolderFiles(config_Folder); err == nil {
		for _, file = range folderfiles {
			if strings.HasSuffix(file.Name(), ".json") && strings.Contains(file.Name(), cnst.XDW_DEFINITION_FILE) {
				if xdwdef, xdwbytes, err := NewWorkflowDefinitionFromFile(file); err == nil {
					if err = DeleteTukWorkflowSubscriptions(xdwdef); err == nil {
						if err = DeleteTukWorkflowDefinition(xdwdef); err == nil {
							pwExps := GetXDWBrokerExpressions(xdwdef)
							pwSubs := Subscriptions{}
							if pwSubs, err = CreateSubscriptionsFromBrokerExpressions(pwExps); err == nil {
								rspSubs.Subscriptions = append(rspSubs.Subscriptions, pwSubs.Subscriptions...)
								rspSubs.Count = rspSubs.Count + pwSubs.Count
								rspSubs.LastInsertId = pwSubs.LastInsertId
								var xdwdefBytes = make(map[string][]byte)
								xdwdefBytes[xdwdef.Ref] = xdwbytes
								PersistXDWDefinitions(xdwdefBytes)
							}
						}
					}
				}
			}
		}
	}
	if err != nil {
		log.Println(err.Error())
	}
	return rspSubs, err
}
func PersistXDWDefinitions(xdwdefs map[string][]byte) error {
	cnt := 0
	for ref, def := range xdwdefs {
		if ref != "" {
			log.Println("Persisting XDW Definition for Pathway : " + ref)
			xdws := XDWS{Action: "insert"}
			xdw := XDW{Name: ref, IsXDSMeta: false, XDW: string(def)}
			xdws.XDW = append(xdws.XDW, xdw)
			if err := xdws.NewTukDBEvent(); err == nil {
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
func CreateSubscriptionsFromBrokerExpressions(brokerExps map[string]string) (Subscriptions, error) {
	log.Printf("Creating %v Broker Subscription", len(brokerExps))
	var err error
	var rspSubs = Subscriptions{Action: "insert"}
	for exp, pwy := range brokerExps {
		log.Printf("Creating Broker Subscription for %s workflow expression %s", pwy, exp)

		dsub := DSUBSubscribe{
			BrokerUrl:   DSUB_BROKER_URL,
			ConsumerUrl: DSUB_CONSUMER_URL,
			Topic:       cnst.DSUB_TOPIC_TYPE_CODE,
			Expression:  exp,
		}
		if err = dsub.NewEvent(); err != nil {
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
			if err = tuksubs.NewTukDBEvent(); err != nil {
				log.Println(err.Error())
			} else {
				tuksub.Id = int(tuksubs.LastInsertId)
				tuksub.Created = util.Tuk_Time()
				rspSubs.Subscriptions = append(rspSubs.Subscriptions, tuksub)
				rspSubs.Count = rspSubs.Count + 1
				rspSubs.LastInsertId = int64(tuksub.Id)
			}
		} else {
			log.Printf("Broker Reference %s in response is invalid", dsub.BrokerRef)
		}
	}
	return rspSubs, err
}
func GetXDWBrokerExpressions(xdwdef WorkflowDefinition) map[string]string {
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
func DeleteTukWorkflowDefinition(xdwdef WorkflowDefinition) error {
	var err error
	var body []byte
	activexdws := TUKXDWS{Action: cnst.DELETE}
	activexdw := TUKXDW{Name: xdwdef.Ref}
	activexdws.XDW = append(activexdws.XDW, activexdw)
	if body, err = json.Marshal(activexdws); err == nil {
		log.Printf("Deleting TUK Workflow Definition for %s workflow", xdwdef.Ref)
		if _, err = newTUKDBRequest(http.MethodPost, cnst.TUK_DB_TABLE_XDWS, body); err == nil {
			log.Printf("Deleted TUK Workflow Definition for %s workflow", xdwdef.Ref)
		}
	}
	if err != nil {
		log.Println(err.Error())
	}
	return err
}
func DeleteTukWorkflowSubscriptions(xdwdef WorkflowDefinition) error {
	var err error
	var body []byte
	activesubs := Subscriptions{Action: cnst.DELETE}
	activesub := Subscription{Pathway: xdwdef.Ref}
	activesubs.Subscriptions = append(activesubs.Subscriptions, activesub)
	if body, err = json.Marshal(activesubs); err == nil {
		log.Printf("Deleting TUK Event Subscriptions for %s workflow", xdwdef.Ref)
		if _, err = newTUKDBRequest(http.MethodPost, cnst.TUK_DB_TABLE_SUBSCRIPTIONS, body); err == nil {
			log.Printf("Deleted TUK Event Subscriptions for %s workflow", xdwdef.Ref)
		}
	}
	if err != nil {
		log.Println(err.Error())
	}
	return err
}
func NewWorkflowDefinitionFromFile(file fs.DirEntry) (WorkflowDefinition, []byte, error) {
	var err error
	var xdwdef = WorkflowDefinition{}
	var xdwdefBytes []byte
	var xdwfile *os.File
	var input = config_Folder + file.Name()
	if xdwfile, err = os.Open(input); err == nil {
		json.NewDecoder(xdwfile).Decode(&xdwdef)
		if xdwdefBytes, err = json.MarshalIndent(xdwdef, "", "  "); err == nil {
			log.Printf("Loaded WF Def for Pathway %s : Bytes = %v", xdwdef.Ref, len(xdwdefBytes))
		}
	}
	if err != nil {
		log.Println(err.Error())
	}
	return xdwdef, xdwdefBytes, err
}

// NewXDWContentCreator takes inputs WokflowDefinition and PIXPatient structs. The author information is taken from the injected event. It creates an IHE XDW content creator actor
// It returns a new IHE XDW complaint document for the workflow and patient
func (i *Event) NewXDWContentCreator(xdwdef WorkflowDefinition, pat PIXPatient) XDWWorkflowDocument {
	log.Printf("Creating New %s Workflow Document for NHS ID %s", xdwdef.Ref, pat.NHSID)
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
	return xdwdoc
}
func PersistWorkflowDocument(workflow XDWWorkflowDocument, workflowdef WorkflowDefinition) error {
	var err error
	var wfDoc []byte
	var wfDef []byte
	persistwf := Workflow{}
	persistwf.Created = util.Tuk_Time()
	persistwf.XDW_Key = workflowdef.Ref + workflow.Patient.ID.Extension
	persistwf.XDW_UID = strings.Split(workflow.WorkflowInstanceId, "^")[0]
	if wfDoc, err = json.Marshal(workflow); err != nil {
		log.Println(err.Error())
		return err
	}
	if wfDef, err = json.Marshal(workflowdef); err != nil {
		log.Println(err.Error())
		return err
	}
	persistwf.XDW_Doc = string(wfDoc)
	persistwf.XDW_Def = string(wfDef)
	existingwfs := Workflows{Action: cnst.SELECT}
	if err := existingwfs.NewTukDBEvent(); err != nil {
		log.Println(err.Error())
		return err
	}
	if existingwfs.Count > 0 {
		for k, exwf := range existingwfs.Workflows {
			if k > 0 {
				if exwf.XDW_Key == workflowdef.Ref+workflow.Patient.ID.Extension {
					updtwfs := Workflows{Action: cnst.UPDATE}
					updtwf := Workflow{
						XDW_Key: exwf.XDW_Key,
						Version: exwf.Version,
					}
					updtwfs.Workflows = append(updtwfs.Workflows, updtwf)
					if err := updtwfs.NewTukDBEvent(); err != nil {
						log.Println(err.Error())
						return err
					}
					log.Println("Deprecated existing workflow")
				}
			}
		}
	}
	persistwfs := Workflows{Action: cnst.INSERT}
	persistwfs.Workflows = append(persistwfs.Workflows, persistwf)
	if err = persistwfs.NewTukDBEvent(); err != nil {
		log.Println(err.Error())
	}
	return err
}
func GetWorkflowEvents(pathway string, nhs string) (Events, error) {
	evs := Events{Action: cnst.SELECT}
	ev := Event{NhsId: nhs, Pathway: pathway, Version: "0"}
	evs.Events = append(evs.Events, ev)
	err := evs.NewTukDBEvent()
	return evs, err
}
func (i *WorkflowDefinition) Log() {
	b, _ := json.MarshalIndent(i, "", "  ")
	log.Println(string(b))
}
func (i *XDWWorkflowDocument) Log() {
	b, _ := xml.MarshalIndent(i, "", "  ")
	log.Println(string(b))
}
func (i *PIXPatient) Log() {
	b, _ := json.MarshalIndent(i, "", "  ")
	log.Println(string(b))
}
func (i *Subscriptions) Log() {
	b, _ := json.MarshalIndent(i, "", "  ")
	log.Println(string(b))
}
func (i *Subscription) Log() {
	b, _ := json.MarshalIndent(i, "", "  ")
	log.Println(string(b))
}
func (i *ClientRequest) Log() {
	b, _ := json.MarshalIndent(i, "", "  ")
	log.Println(string(b))
}
func (i *Event) InitDSUBEvent(dsubNotify DSUBNotifyMessage) {
	i.Creationtime = util.Tuk_Time()
	i.DocName = dsubNotify.NotificationMessage.Message.SubmitObjectsRequest.RegistryObjectList.ExtrinsicObject.Name.LocalizedString.Value
	i.ClassCode = cnst.NO_VALUE
	i.ConfCode = cnst.NO_VALUE
	i.FormatCode = cnst.NO_VALUE
	i.FacilityCode = cnst.NO_VALUE
	i.PracticeCode = cnst.NO_VALUE
	i.Expression = cnst.NO_VALUE
	i.Authors = cnst.NO_VALUE
	i.XdsPid = cnst.NO_VALUE
	i.XdsDocEntryUid = cnst.NO_VALUE
	i.RepositoryUniqueId = cnst.NO_VALUE
	i.NhsId = cnst.NO_VALUE
	i.User = cnst.NO_VALUE
	i.Org = cnst.NO_VALUE
	i.Role = cnst.NO_VALUE
	i.Topic = cnst.NO_VALUE
	i.Pathway = cnst.NO_VALUE
	i.Notes = "None"
	i.Version = "0"
	i.BrokerRef = dsubNotify.NotificationMessage.SubscriptionReference.Address.Text
	i.setRepositoryUniqueId(dsubNotify)
	tukauthors := TukAuthors{}
	for _, c := range dsubNotify.NotificationMessage.Message.SubmitObjectsRequest.RegistryObjectList.ExtrinsicObject.Classification {
		log.Printf("Found Classification Scheme %s", c.ClassificationScheme)
		val := c.Name.LocalizedString.Value
		switch c.ClassificationScheme {
		case cnst.URN_CLASS_CODE:
			i.ClassCode = val
		case cnst.URN_CONF_CODE:
			i.ConfCode = val
		case cnst.URN_FORMAT_CODE:
			i.FormatCode = val
		case cnst.URN_FACILITY_CODE:
			i.FacilityCode = val
		case cnst.URN_PRACTICE_CODE:
			i.PracticeCode = val
		case cnst.URN_TYPE_CODE:
			i.Expression = val
		case cnst.URN_AUTHOR:
			tukauthor := TukAuthor{}
			for _, s := range c.Slot {
				switch s.Name {
				case cnst.AUTHOR_PERSON:
					for _, ap := range s.ValueList.Value {
						tukauthor.Person = tukauthor.Person + util.PrettyAuthorPerson(ap) + ","
					}
					tukauthor.Person = strings.TrimSuffix(tukauthor.Person, ",")
				case cnst.AUTHOR_INSTITUTION:
					for _, ai := range s.ValueList.Value {
						tukauthor.Institution = tukauthor.Institution + util.PrettyAuthorInstitution(ai) + ","
					}
					tukauthor.Institution = strings.TrimSuffix(tukauthor.Institution, ",")
				}
			}
			tukauthors.Author = append(tukauthors.Author, tukauthor)
		default:
			log.Printf("Unknown classication scheme %s. Skipping", c.ClassificationScheme)
		}
	}
	bstr, _ := json.Marshal(tukauthors)
	i.Authors = string(bstr)
	for _, a := range tukauthors.Author {
		if a.Person != "" {
			i.User = i.User + strings.ReplaceAll(a.Person, "^", " ") + ", "
		}
		if a.Institution != "" {
			if strings.Contains(a.Institution, "^") {
				i.Org = strings.Split(a.Institution, "^")[0] + ", "
			} else {
				i.Org = a.Institution + ", "
			}
		}
	}
	i.User = strings.TrimSuffix(i.User, ", ")
	i.Org = strings.TrimSuffix(i.Org, ", ")
	i.Role = i.PracticeCode
	i.setExternalIdentifiers(dsubNotify)
	log.Println("Parsed DSUB Notify Message")
	i.PrintEventVals()
}
func (i *Event) PrintEventVals() {
	log.Printf("Set Event Author Person - %s", i.User)
	log.Printf("Set Event Author Organisation - %s", i.Org)
	log.Printf("Set Event Author Role:%s", i.Role)
	log.Printf("Set Event Creation Time - %s", i.Creationtime)
	log.Printf("Set Document Name - %s", i.DocName)
	log.Println("Set Patient Reg ID - " + i.XdsPid)
	log.Printf("Set Repository Unique ID - %s", i.RepositoryUniqueId)
	log.Printf("Set Document Unique ID - %s", i.XdsDocEntryUid)
	log.Printf("Set ClassCode:%s", i.ClassCode)
	log.Printf("Set ConfCode:%s", i.ConfCode)
	log.Printf("Set FormatCode:%s", i.FormatCode)
	log.Printf("Set FacilityCode:%s", i.FacilityCode)
	log.Printf("Set PracticeCode:%s", i.PracticeCode)
	log.Printf("Set TypeCode:%s", i.Expression)
}
func (i *Event) setRepositoryUniqueId(dsubNotify DSUBNotifyMessage) {
	log.Println("Searching for Repository Unique ID")
	for _, slot := range dsubNotify.NotificationMessage.Message.SubmitObjectsRequest.RegistryObjectList.ExtrinsicObject.Slot {
		if slot.Name == cnst.REPOSITORY_UID {
			i.RepositoryUniqueId = slot.ValueList.Value[0]
			return
		}
	}
}
func (i *Event) setExternalIdentifiers(dsubNotify DSUBNotifyMessage) {
	for exid := range dsubNotify.NotificationMessage.Message.SubmitObjectsRequest.RegistryObjectList.ExtrinsicObject.ExternalIdentifier {
		val := dsubNotify.NotificationMessage.Message.SubmitObjectsRequest.RegistryObjectList.ExtrinsicObject.ExternalIdentifier[exid].Value
		ids := dsubNotify.NotificationMessage.Message.SubmitObjectsRequest.RegistryObjectList.ExtrinsicObject.ExternalIdentifier[exid].IdentificationScheme
		switch ids {
		case cnst.URN_XDS_PID:
			i.XdsPid = strings.Split(val, "^^^")[0]
		case cnst.URN_XDS_DOCUID:
			i.XdsDocEntryUid = val
		}
	}
}
func NewDSUBAckMessage() []byte {
	return []byte(DSUB_ACK_TEMPLATE)
}
func (i *DSUBSubscribe) NewEvent() error {
	var err error
	var tmplt *template.Template
	if tmplt, err = template.New(cnst.SUBSCRIBE).Funcs(util.TemplateFuncMap()).Parse(DSUB_SUBSCRIBE_TEMPLATE); err == nil {
		var b bytes.Buffer
		if err = tmplt.Execute(&b, i); err == nil {
			i.BrokerUrl = DSUB_BROKER_URL
			i.Request = b.Bytes()
			var resp *http.Response
			var rsp []byte
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5000)*time.Millisecond)
			defer cancel()
			if resp, err = newSOAPRequest(i.BrokerUrl, cnst.SOAP_ACTION_SUBSCRIBE_REQUEST, i.Request, ctx); err == nil {
				if rsp, err = io.ReadAll(resp.Body); err == nil {
					log.Println(string(rsp))
					subrsp := DSUBSubscribeResponse{}
					if err = xml.Unmarshal(rsp, &subrsp); err == nil {
						i.BrokerRef = subrsp.Body.SubscribeResponse.SubscriptionReference.Address
						log.Printf("Broker Response. Broker Ref :  %s", subrsp.Body.SubscribeResponse.SubscriptionReference.Address)
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
func (i *DSUBCancel) NewEvent() error {
	tmplt, err := template.New(cnst.CANCEL).Funcs(util.TemplateFuncMap()).Parse(DSUB_CANCEL_TEMPLATE)
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
	err = i.CancelSubscription()
	if err != nil {
		log.Println(err.Error())
	}
	return err
}
func (i *DSUBCancel) CancelSubscription() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5000)*time.Millisecond)
	defer cancel()
	_, err := newSOAPRequest(DSUB_BROKER_URL, cnst.SOAP_ACTION_UNSUBSCRIBE_REQUEST, i.Request, ctx)
	if err != nil {
		log.Println(err.Error())
	}
	return err
}
func (i *PIXmQuery) InitPIXPatient() error {
	url := PIX_MANAGER_URL + "?identifier=" + i.PIDOID + "%7C" + i.PID + "&_format=json&_pretty=true"
	log.Println("GET Patient URL:" + url)
	req, _ := http.NewRequest(cnst.HTTP_GET, url, nil)
	req.Header.Set(cnst.CONTENT_TYPE, cnst.APPLICATION_JSON)
	req.Header.Set(cnst.ACCEPT, cnst.ALL)
	req.Header.Set(cnst.CONNECTION, cnst.KEEP_ALIVE)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(2000)*time.Millisecond)
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
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
func (i *XDWS) NewTukDBEvent() error {
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
func (i *Workflows) NewTukDBEvent() error {
	log.Printf("Sending %s Request to %s", getHttpMethod(i.Action), TUK_DB_URL+"workflows")
	body, _ := json.Marshal(i)
	bodyBytes, err := newTUKDBRequest(getHttpMethod(i.Action), "workflows", body)
	if err == nil {
		if err := json.Unmarshal(bodyBytes, &i); err != nil {
			log.Println(err.Error())
		}
	}
	return err
}
func (i *Subscriptions) NewTukDBEvent() error {
	log.Printf("Sending %s Request to %s", getHttpMethod(i.Action), TUK_DB_URL+cnst.SUBSCRIPTIONS)
	body, _ := json.Marshal(i)
	bodyBytes, err := newTUKDBRequest(getHttpMethod(i.Action), cnst.SUBSCRIPTIONS, body)
	if err == nil {
		if err := json.Unmarshal(bodyBytes, &i); err != nil {
			fmt.Println(err.Error())
		}
	}
	return err
}
func (i *Events) NewTukDBEvent() error {
	log.Printf("Sending %s Request to %s", getHttpMethod(i.Action), TUK_DB_URL+cnst.EVENTS)
	body, _ := json.Marshal(i)
	bodyBytes, err := newTUKDBRequest(getHttpMethod(i.Action), cnst.EVENTS, body)
	if err == nil {
		if err := json.Unmarshal(bodyBytes, &i); err != nil {
			fmt.Println(err.Error())
		}
	}
	return err
}
func (i *IDMaps) NewTukDBEvent() error {
	log.Printf("Sending %s Request to %s", getHttpMethod(i.Action), TUK_DB_URL+cnst.ID_MAPS)
	body, _ := json.Marshal(i)
	bodyBytes, err := newTUKDBRequest(getHttpMethod(i.Action), cnst.ID_MAPS, body)
	if err == nil {
		if err := json.Unmarshal(bodyBytes, &i); err != nil {
			fmt.Println(err.Error())
		}
	}
	return err
}
func getHttpMethod(action string) string {
	switch action {
	case cnst.SELECT:
		return cnst.HTTP_GET
	default:
		return cnst.HTTP_POST
	}
}
func newTUKDBRequest(httpMethod string, resource string, body []byte) ([]byte, error) {
	var err error
	var req *http.Request
	var resp *http.Response
	var bodyBytes []byte
	client := &http.Client{}
	if req, err = http.NewRequest(httpMethod, TUK_DB_URL+resource, bytes.NewBuffer(body)); err == nil {
		req.Header.Add(cnst.CONTENT_TYPE, cnst.APPLICATION_JSON_CHARSET_UTF_8)
		if resp, err = client.Do(req); err == nil {
			log.Printf("Response Status Code %v\n", resp.StatusCode)
			if resp.StatusCode == http.StatusOK {
				if bodyBytes, err = io.ReadAll(resp.Body); err == nil {
					return bodyBytes, err
				}
			}
		}
	}
	log.Println(err.Error())
	return bodyBytes, err
}
func newSOAPRequest(url string, soapAction string, body []byte, ctx context.Context) (*http.Response, error) {
	var err error
	var req *http.Request
	var resp *http.Response
	if req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(string(body))); err == nil {
		req.Header.Set(cnst.SOAP_ACTION, soapAction)
		req.Header.Set(cnst.CONTENT_TYPE, cnst.SOAP_XML)
		req.Header.Set(cnst.ACCEPT, cnst.ALL)
		req.Header.Set(cnst.CONNECTION, cnst.KEEP_ALIVE)
		resp, err = http.DefaultClient.Do(req.WithContext(ctx))
	}
	return resp, err
}

// type eventsList []Event

// func (e eventsList) Len() int {
// 	return len(e)
// }
// func (e eventsList) Less(i, j int) bool {
// 	return e[i].EventId > e[j].EventId
// }
// func (e eventsList) Swap(i, j int) {
// 	e[i], e[j] = e[j], e[i]
// }
