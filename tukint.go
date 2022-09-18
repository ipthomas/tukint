package tukint

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	cnst "github.com/ipthomas/tukcnst"
	dbint "github.com/ipthomas/tukdbint"
	dsub "github.com/ipthomas/tukdsub"
	"github.com/ipthomas/tukhttp"
	pdq "github.com/ipthomas/tukpdq"
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
	Complete   int
	Closed     int
	ServerURL  string
}
type WorkflowState struct {
	Events    dbint.Events    `json:"events"`
	XDWS      TUKXDWS         `json:"xdws"`
	Workflows dbint.Workflows `json:"workflows"`
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
	HOME_COMMUNITY_ID                  = "1.2.3.4.5"
	is_Secure                          = false
	http_Scheme                        = "http://"
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

func Set_Home_Community(homeCommunityId string) {
	HOME_COMMUNITY_ID = homeCommunityId
}
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
func InitCodeSystem() {
	util.LoadCodeSystemFile(codeSystem_File)
}

// SetIsSecure sets the Tuk Server http_Scheme to https if true or http is false. Default is false
func SetIsSecure(isSecure bool) {
	is_Secure = isSecure
	if is_Secure {
		http_Scheme = "https://"
	} else {
		http_Scheme = "http://"
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

// InitLog calls tukutils.CreateLog(logFolder) which checks if the log folder exists and creates it if not. If no log folder has been set it defaults to `basepath/logs/` It then checks for a subfolder for the current year i.e. 2022 and creates it if it does not exist. It then checks for a log file with a name equal to the current day and month and extension .log i.e. 0905.log. If it exists log output is appended to the existing file otherwise a new log file is created.
func InitLog() {
	logFile = util.CreateLog(log_Folder)
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
func NewHTTPServer(basefolder string, logfolder string, configfolder string, templatefolder string, codesystemfile string, baseresourceurl string, port string) {
	srv := TukHttpServer{
		BaseFolder:      basefolder,
		ConfigFolder:    configfolder,
		TemplateFolder:  templatefolder,
		LogFolder:       logfolder,
		LogToFile:       logfolder != "",
		CodeSystemFile:  codesystemfile,
		BaseResourceUrl: baseresourceurl,
		Port:            port,
	}
	srv.NewHTTPServer()
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
	http.HandleFunc(baseResourceUrl, util.WriteResponseHeaders(route_TUK_Server_Request))
	log.Printf("Initialised HTTP Server - Listening on %s", GetServerURL())
	util.MonitorApp()
	log.Fatal(http.ListenAndServe(port, nil))

}

func GetServerURL() string {
	return http_Scheme + hostname + port + baseResourceUrl
}
func InitXDWWorkflowDocument(tukwf dbint.Workflow) (XDWWorkflowDocument, error) {
	var err error
	xdwStruc := XDWWorkflowDocument{}
	err = json.Unmarshal([]byte(tukwf.XDW_Doc), &xdwStruc)
	return xdwStruc, err
}
func InitXDWDefinition(tukwf dbint.Workflow) (WorkflowDefinition, error) {
	var err error
	xdwdef := WorkflowDefinition{}
	err = json.Unmarshal([]byte(tukwf.XDW_Def), &xdwdef)
	return xdwdef, err
}
func route_TUK_Server_Request(rsp http.ResponseWriter, r *http.Request) {
	req := ClientRequest{ServerURL: GetServerURL()}
	if err := req.parseHTTPRequest(r); err == nil {
		Log(req)
		rsp.Write([]byte(req.ProcessClientRequest()))
	} else {
		log.Println(err.Error())
	}
}
func (i *ClientRequest) parseHTTPRequest(req *http.Request) error {
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
	if len(i.XDWKey) > 12 {
		i.Pathway, i.NHS = util.SplitXDWKey(i.XDWKey)
	}
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
	tukpdq := pdq.PDQQuery{
		Server:     cnst.PIXm,
		Server_URL: PIX_MANAGER_URL,
		NHS_ID:     i.NHS,
		REG_OID:    REGIONAL_OID,
	}
	if err := pdq.PDQ(&tukpdq); err != nil {
		return err.Error()
	}
	var b bytes.Buffer
	if err := htmlTemplates.ExecuteTemplate(&b, "pixpatient", tukpdq.Response); err != nil {
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

	wfs := dbint.Workflows{Action: cnst.SELECT}
	wf := dbint.Workflow{XDW_Key: i.Pathway, Version: i.Version}
	wfs.Workflows = append(wfs.Workflows, wf)
	if err := AWS_Workflows_API_Request(&wfs); err != nil {
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
		Patient   pdq.PIXPatient
	}
	type TmpltWorkflows struct {
		Count     int
		ServerURL string
		Workflows []TmpltWorkflow
	}
	tmpltwfs := TmpltWorkflows{ServerURL: i.ServerURL}

	tukwfs := dbint.Workflows{Action: cnst.SELECT}
	if err := AWS_Workflows_API_Request(&tukwfs); err != nil {
		log.Println(err.Error())
		return err.Error()
	}
	log.Printf("Processing %v workflows", tukwfs.Count)
	for _, wf := range tukwfs.Workflows {
		if wf.Id > 0 {
			pat := pdq.PIXPatient{}
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
			tukpdq := pdq.PDQQuery{
				Server:     cnst.PIXm,
				Server_URL: PIX_MANAGER_URL,
				NHS_ID:     i.NHS,
				REG_OID:    REGIONAL_OID,
			}
			if err := pdq.PDQ(&tukpdq); err != nil {
				log.Println(err.Error())
				continue
			}
			if len(pat.NHSID) != 10 {
				log.Println("Unable to obtain valid patient details")
				continue
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
		i.XDWKey = i.Pathway + i.NHS
	}
	xdw := XDWWorkflowDocument{}
	wfs := dbint.Workflows{Action: cnst.SELECT}
	wf := dbint.Workflow{XDW_Key: i.XDWKey, Version: i.Version}

	wfs.Workflows = append(wfs.Workflows, wf)
	AWS_Workflows_API_Request(&wfs)

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
	wfs := dbint.Workflows{Action: cnst.SELECT}
	AWS_Workflows_API_Request(&wfs)
	log.Printf("Processing %v workflows", wfs.Count)
	for _, wf := range wfs.Workflows {
		dashboard.Total = dashboard.Total + 1
		xdw, err := InitXDWWorkflowDocument(wf)
		if err != nil {
			continue
		}
		json.Unmarshal([]byte(wf.XDW_Doc), &xdw)
		log.Printf("Workflow Created on %s for Patient NHS ID %s Workflow Status %s Workflow Version %v", xdw.EffectiveTime.Value, xdw.Patient.ID.Extension, xdw.WorkflowStatus, wf.Version)
		switch xdw.WorkflowStatus {
		case "READY":
			dashboard.Ready = dashboard.Ready + 1
		case "OPEN":
			dashboard.Open = dashboard.Open + 1
		case "IN_PROGRESS":
			dashboard.InProgress = dashboard.InProgress + 1
		case "COMPLETE":
			dashboard.Complete = dashboard.Complete + 1
		case "CLOSED":
			dashboard.Closed = dashboard.Closed + 1
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
	InitLambdaVars()
	dsubEvent := dsub.DSUBEvent{Message: i.Message}
	return dsub.NewDsubEvent(&dsubEvent)
}
func UpdateWorkflow(i *dbint.Event, pat pdq.PIXPatient) {
	log.Printf("Updating %s Workflow for patient %s %s %s", i.Pathway, pat.GivenName, pat.FamilyName, i.NhsId)
	tukwfdefs := dbint.XDWS{Action: cnst.SELECT}
	tukwfdef := dbint.XDW{Name: i.Pathway}
	tukwfdefs.XDW = append(tukwfdefs.XDW, tukwfdef)
	if err := AWS_XDWs_API_Request(&tukwfdefs); err != nil {
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

		log.Printf("Searching for existing workflow for %s %s", i.Pathway, i.NhsId)
		tukwfdocs := dbint.Workflows{Action: cnst.SELECT}
		tukwfdoc := dbint.Workflow{XDW_Key: i.Pathway + i.NhsId}
		tukwfdocs.Workflows = append(tukwfdocs.Workflows, tukwfdoc)
		if err := AWS_Workflows_API_Request(&tukwfdocs); err != nil {
			log.Println(err.Error())
			return
		}
		if tukwfdocs.Count == 0 {
			log.Printf("No existing workflow state found for %s %s", i.Pathway, i.NhsId)
			newWorkflow := NewXDWContentCreator(i.User, "", i.Org, util.GetCodeSystemVal(i.Org), wfdef, pat)
			log.Println("Persisting Workflow state")
			if err := PersistWorkflowDocument(newWorkflow, wfdef); err != nil {
				log.Println(err.Error())
				return
			}
			log.Println("Workflow state persisted")
			Log(newWorkflow)
		} else {
			log.Printf("Existing Workflow state found for Pathway %s NHS ID %s", i.Pathway, i.NhsId)
		}
	} else {
		log.Printf("Warning. No XDW Definition found for pathway %s", i.Pathway)
	}
}

func NewXDWContentUpdator(i *dbint.Event, wfdef WorkflowDefinition, wf XDWWorkflowDocument, pat pdq.PIXPatient) {
	log.Printf("Updating %s Workflow for NHS ID %s with latest events", wf.WorkflowDefinitionReference, pat.NHSID)
	if wf.WorkflowStatus == cnst.COMPLETE || wf.WorkflowStatus == "CLOSED" {
		log.Printf("Workflow state is %s.", wf.WorkflowStatus)
		return
	}
	pwy, nhs := util.SplitXDWKey(wf.WorkflowDefinitionReference)
	tukEvents := dbint.Events{Action: cnst.SELECT}
	tukEvent := dbint.Event{Pathway: pwy, NhsId: nhs}
	tukEvents.Events = append(tukEvents.Events, tukEvent)
	if err := AWS_Events_API_Request(&tukEvents); err != nil {
		log.Println(err.Error())
		return
	}
	sort.Sort(eventsList(tukEvents.Events))
	util.Log(tukEvents)
	log.Printf("Updating %s Workflow Tasks with %v Events", wf.WorkflowDefinitionReference, len(tukEvents.Events))
	for _, ev := range tukEvents.Events {
		for k, wfdoctask := range wf.TaskList.XDWTask {
			log.Println("Checking Workflow Document Task " + wfdoctask.TaskData.TaskDetails.Name + " for matching Events")
			for inp, input := range wfdoctask.TaskData.Input {
				if ev.Expression == input.Part.Name {
					log.Println("Matched workflow document task " + wfdoctask.TaskData.TaskDetails.ID + " Input Part : " + input.Part.Name + " with Event Expression : " + ev.Expression + " Status : " + wfdoctask.TaskData.TaskDetails.Status)

					wf.TaskList.XDWTask[k].TaskData.TaskDetails.LastModifiedTime = util.Time_Now()
					wf.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.AttachedTime = time.Now().Format(time.RFC3339)
					wf.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.AttachedBy = ev.User + " " + ev.Org + " " + ev.Role
					wf.TaskList.XDWTask[k].TaskData.TaskDetails.Status = "REQUESTED"
					wf.TaskList.XDWTask[k].TaskData.TaskDetails.ActualOwner = ev.User + " " + ev.Org + " " + ev.Role
					if strings.HasSuffix(wfdoctask.TaskData.Input[inp].Part.AttachmentInfo.AccessType, "XDSregistered") {
						wf.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.Identifier = ev.RepositoryUniqueId + ":" + ev.XdsDocEntryUid
						wf.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.HomeCommunityId = REGIONAL_OID
						wf.NewWorkflowTaskEvent(ev, k)
					} else {
						wf.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.Identifier = strconv.Itoa(int(ev.EventId))
						wf.NewWorkflowTaskEvent(ev, k)
					}
					wf.WorkflowStatus = "IN_PROGRESS"
				}
			}
			for oup, output := range wf.TaskList.XDWTask[k].TaskData.Output {
				if ev.Expression == output.Part.Name {
					log.Println("Matched workflow document task " + wfdoctask.TaskData.TaskDetails.ID + " Output Part : " + output.Part.Name + " with Event Expression : " + ev.Expression + " Status : " + wfdoctask.TaskData.TaskDetails.Status)

					wf.TaskList.XDWTask[k].TaskData.TaskDetails.LastModifiedTime = util.Time_Now()
					wf.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.AttachedTime = util.Time_Now()
					wf.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.AttachedBy = ev.User + " " + ev.Org + " " + ev.Role
					wf.TaskList.XDWTask[k].TaskData.TaskDetails.ActualOwner = ev.User + " " + ev.Org + " " + ev.Role
					wf.TaskList.XDWTask[k].TaskData.TaskDetails.Status = "IN_PROGRESS"
					if strings.HasSuffix(wfdoctask.TaskData.Output[oup].Part.AttachmentInfo.AccessType, "XDSregistered") {
						wf.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.Identifier = ev.RepositoryUniqueId + ":" + ev.XdsDocEntryUid
						wf.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.HomeCommunityId = REGIONAL_OID
						wf.NewWorkflowTaskEvent(ev, k)
					} else {
						wf.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.Identifier = strconv.Itoa(int(ev.EventId))
						wf.NewWorkflowTaskEvent(ev, k)
					}
					wf.WorkflowStatus = "IN_PROGRESS"

				}
			}
		}
	}
	for task := range wf.TaskList.XDWTask {
		if wf.TaskList.XDWTask[task].TaskData.TaskDetails.Status != "COMPLETE" {
			if isTaskCompleteBehaviorMet(wf, wfdef, task) {
				wf.TaskList.XDWTask[task].TaskData.TaskDetails.Status = "COMPLETE"
			}
		}
	}
	for task := range wf.TaskList.XDWTask {
		if wf.TaskList.XDWTask[task].TaskData.TaskDetails.Status != "COMPLETE" {
			if isTaskCompleteBehaviorMet(wf, wfdef, task) {
				wf.TaskList.XDWTask[task].TaskData.TaskDetails.Status = cnst.COMPLETE
			}
		}
	}
	if isWorkflowCompleteBehaviorMet(wf, wfdef) {
		wf.WorkflowStatus = cnst.COMPLETE
		docevent := DocumentEvent{}
		docevent.Author = i.User
		docevent.TaskEventIdentifier = util.Newid()
		docevent.EventTime = i.Creationtime
		docevent.EventType = cnst.CLOSED
		docevent.PreviousStatus = wf.WorkflowStatusHistory.DocumentEvent[len(wf.WorkflowStatusHistory.DocumentEvent)-1].ActualStatus
		docevent.ActualStatus = cnst.COMPLETE
		wf.WorkflowStatusHistory.DocumentEvent = append(wf.WorkflowStatusHistory.DocumentEvent, docevent)
		for k := range wf.TaskList.XDWTask {
			wf.TaskList.XDWTask[k].TaskData.TaskDetails.Status = cnst.COMPLETE
		}
		log.Println("Closed Workflow. Total Workflow Document Events " + strconv.Itoa(len(wf.WorkflowStatusHistory.DocumentEvent)))
	} else {
		log.Println("Workflow Completion Behaviour not met")
	}
}
func isWorkflowCompleteBehaviorMet(wf XDWWorkflowDocument, wfdef WorkflowDefinition) bool {
	var conditions []string
	var completedConditions = 0
	for _, behaviour := range wfdef.CompletionBehavior {
		condition := behaviour.Completion.Condition
		if condition != "" {
			if strings.Contains(condition, " and ") {
				conditions = strings.Split(condition, " and ")
			} else {
				conditions = append(conditions, condition)
			}
			for _, condition := range conditions {
				log.Println("Checking Workflow Completion Condition " + condition)
				endMethodInd := strings.Index(condition, "(")
				if endMethodInd > 0 {
					method := condition[0:endMethodInd]
					if method != cnst.TASK {
						log.Println(method + " is an Invalid Workflow Completion Behaviour Condition method. Ignoring Condition")
						continue
					}
					endParamInd := strings.Index(condition, ")")
					param := condition[endMethodInd+1 : endParamInd]
					for _, task := range wf.TaskList.XDWTask {
						if task.TaskData.TaskDetails.ID == param {
							if task.TaskData.TaskDetails.Status == "COMPLETE" {
								completedConditions = completedConditions + 1
							}
						}
					}
				}
			}
		}
	}
	return len(conditions) == completedConditions
}
func isTaskCompleteBehaviorMet(wf XDWWorkflowDocument, wfdef WorkflowDefinition, task int) bool {
	for _, cond := range wfdef.Tasks[task].CompletionBehavior {
		var conditions []string
		var completedConditions = 0

		if cond.Completion.Condition != "" {
			if strings.Contains(cond.Completion.Condition, " and ") {
				conditions = strings.Split(cond.Completion.Condition, " and ")
			} else {
				conditions = append(conditions, cond.Completion.Condition)
			}
			for _, condition := range conditions {
				endMethodInd := strings.Index(condition, "(")
				if endMethodInd > 0 {
					method := condition[0:endMethodInd]
					endParamInd := strings.Index(condition, ")")
					if endParamInd < endMethodInd+2 {
						log.Println("Invalid Condition. End bracket index invalid")
						continue
					}
					param := condition[endMethodInd+1 : endParamInd]
					switch method {
					case "output":
						for _, op := range wf.TaskList.XDWTask[task].TaskData.Output {
							if op.Part.AttachmentInfo.AttachedTime != "" && op.Part.AttachmentInfo.Name == param {
								completedConditions = completedConditions + 1
							}
						}
					case "input":
						for _, in := range wf.TaskList.XDWTask[task].TaskData.Input {
							if in.Part.AttachmentInfo.AttachedTime != "" && in.Part.AttachmentInfo.Name == param {
								completedConditions = completedConditions + 1
							}
						}
					case "task":
						for _, task := range wf.TaskList.XDWTask {
							if task.TaskData.TaskDetails.ID == param {
								if task.TaskData.TaskDetails.Status == "COMPLETE" {
									completedConditions = completedConditions + 1
								}
							}
						}
					}
				}
			}
			if len(conditions) == completedConditions {
				return true
			}
		}
	}
	return false
}
func (i *XDWWorkflowDocument) NewWorkflowHistoryEvent(event dbint.Event) {
	docevent := DocumentEvent{}
	docevent.Author = event.User
	docevent.TaskEventIdentifier = strconv.Itoa(int(event.EventId))
	docevent.EventTime = util.Time_Now()
	docevent.EventType = event.Expression
	if len(i.WorkflowStatusHistory.DocumentEvent) > 0 {
		docevent.PreviousStatus = cnst.READY
	} else {
		docevent.PreviousStatus = cnst.IN_PROGRESS
	}
	docevent.ActualStatus = cnst.IN_PROGRESS
	i.WorkflowStatusHistory.DocumentEvent = append(i.WorkflowStatusHistory.DocumentEvent, docevent)
}
func (i *XDWWorkflowDocument) NewWorkflowTaskEvent(event dbint.Event, task int) {
	nte := TaskEvent{
		ID:         strconv.Itoa(len(i.TaskList.XDWTask[task].TaskEventHistory.TaskEvent) + 1),
		EventTime:  util.Time_Now(),
		Identifier: strconv.Itoa(int(event.EventId)),
		EventType:  event.Expression,
		Status:     cnst.COMPLETE,
	}
	i.TaskList.XDWTask[task].TaskEventHistory.TaskEvent = append(i.TaskList.XDWTask[task].TaskEventHistory.TaskEvent, nte)
}
func (i *XDWWorkflowDocument) UpdateXDWWorkflowDocument(events dbint.Events) {
	for _, event := range events.Events {
		for _, task := range i.TaskList.XDWTask {
			for _, inp := range task.TaskData.Input {
				if event.Expression == inp.Part.AttachmentInfo.Name {
					inp.Part.AttachmentInfo.Identifier = event.RepositoryUniqueId + ":" + event.XdsDocEntryUid
					inp.Part.AttachmentInfo.AttachedBy = event.User
					inp.Part.AttachmentInfo.AttachedTime = util.Time_Now()
					inp.Part.AttachmentInfo.HomeCommunityId = HOME_COMMUNITY_ID
				}
			}
			for _, out := range task.TaskData.Input {
				if event.Expression == out.Part.AttachmentInfo.Name {
					out.Part.AttachmentInfo.Identifier = event.RepositoryUniqueId + ":" + event.XdsDocEntryUid
					out.Part.AttachmentInfo.AttachedBy = event.User
					out.Part.AttachmentInfo.AttachedTime = util.Time_Now()
					out.Part.AttachmentInfo.HomeCommunityId = HOME_COMMUNITY_ID
				}
			}
		}
	}
}

// New XDWDefinition takes an input string containing the workflow ref. It returns a WorkflowDefinition struc for the requested workflow
func NewXDWDefinition(workflow string) (WorkflowDefinition, error) {
	var err error
	xdwdef := WorkflowDefinition{}
	xdws := dbint.XDWS{Action: cnst.SELECT}
	xdw := dbint.XDW{Name: workflow}
	xdws.XDW = append(xdws.XDW, xdw)
	err = dbint.NewDBEvent(&xdws)
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
func NewXDWContentCreator(author string, authorPrefix string, authorOrg string, authorOID string, xdwdef WorkflowDefinition, pat pdq.PIXPatient) XDWWorkflowDocument {
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
	xdwdoc.EffectiveTime.Value = util.Time_Now()
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

// RegisterXDWDefinitions loads and parses xdw definition files (with suffix `_xdwdef.json“) in the config_folder. If input param folder == "", the value that is set in the global var config_folder is used.
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
func RegisterXDWDefinitions(folder string) (dbint.Subscriptions, error) {
	var folderfiles []fs.DirEntry
	var file fs.DirEntry
	var err error
	var rspSubs = dbint.Subscriptions{Action: cnst.INSERT}
	if folder != "" {
		config_Folder = folder
	}
	if folderfiles, err = util.GetFolderFiles(config_Folder); err == nil {
		for _, file = range folderfiles {
			if strings.HasSuffix(file.Name(), ".json") && strings.Contains(file.Name(), cnst.XDW_DEFINITION_FILE) {
				if xdwdef, xdwbytes, err := NewWorkflowDefinitionFromFile(file); err == nil {
					if err = DeleteTukWorkflowSubscriptions(xdwdef); err == nil {
						if err = DeleteTukWorkflowDefinition(xdwdef); err == nil {
							pwExps := GetXDWBrokerExpressions(xdwdef)
							pwSubs := dbint.Subscriptions{}
							if pwSubs, err = CreateSubscriptionsFromBrokerExpressions(pwExps); err == nil {
								rspSubs.Subscriptions = append(rspSubs.Subscriptions, pwSubs.Subscriptions...)
								rspSubs.Count = rspSubs.Count + pwSubs.Count
								rspSubs.LastInsertId = pwSubs.LastInsertId
								var xdwdefBytes = make(map[string][]byte)
								xdwdefBytes[xdwdef.Ref] = xdwbytes
								PersistXDWDefinitions(xdwdefBytes)
								if err := os.Rename(config_Folder+"/"+file.Name(), config_Folder+"/"+file.Name()+".deployed"); err != nil {
									log.Println(err.Error())
								}
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
			xdws := dbint.XDWS{Action: "insert"}
			xdw := dbint.XDW{Name: ref, IsXDSMeta: false, XDW: string(def)}
			xdws.XDW = append(xdws.XDW, xdw)
			if err := dbint.NewDBEvent(&xdws); err == nil {
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
func CreateSubscriptionsFromBrokerExpressions(brokerExps map[string]string) (dbint.Subscriptions, error) {
	log.Printf("Creating %v Broker Subscription", len(brokerExps))
	var err error
	var rspSubs = dbint.Subscriptions{Action: "insert"}
	for exp, pwy := range brokerExps {
		log.Printf("Creating Broker Subscription for %s workflow expression %s", pwy, exp)

		sub := dsub.DSUBSubscribe{
			BrokerUrl:   DSUB_BROKER_URL,
			ConsumerUrl: DSUB_CONSUMER_URL,
			Topic:       cnst.DSUB_TOPIC_TYPE_CODE,
			Expression:  exp,
		}
		if err = dsub.NewDsubEvent(&sub); err != nil {
			return rspSubs, err
		}
		if sub.BrokerRef != "" {
			tuksub := dbint.Subscription{
				BrokerRef:  sub.BrokerRef,
				Pathway:    pwy,
				Topic:      cnst.DSUB_TOPIC_TYPE_CODE,
				Expression: exp,
			}
			tuksubs := dbint.Subscriptions{Action: cnst.INSERT}
			tuksubs.Subscriptions = append(tuksubs.Subscriptions, tuksub)
			log.Println("Registering Subscription Reference with Event Service")
			if err = dbint.NewDBEvent(&tuksubs); err != nil {
				log.Println(err.Error())
			} else {
				tuksub.Id = int(tuksubs.LastInsertId)
				tuksub.Created = util.Time_Now()
				rspSubs.Subscriptions = append(rspSubs.Subscriptions, tuksub)
				rspSubs.Count = rspSubs.Count + 1
				rspSubs.LastInsertId = int64(tuksub.Id)
			}
		} else {
			log.Printf("Broker Reference %s in response is invalid", sub.BrokerRef)
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
		if _, err = newAWS_APIRequest(http.MethodPost, cnst.TUK_DB_TABLE_XDWS, body); err == nil {
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
	activesubs := dbint.Subscriptions{Action: cnst.DELETE}
	activesub := dbint.Subscription{Pathway: xdwdef.Ref}
	activesubs.Subscriptions = append(activesubs.Subscriptions, activesub)
	if body, err = json.Marshal(activesubs); err == nil {
		log.Printf("Deleting TUK Event Subscriptions for %s workflow", xdwdef.Ref)
		if _, err = newAWS_APIRequest(http.MethodPost, cnst.TUK_DB_TABLE_SUBSCRIPTIONS, body); err == nil {
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
	var input = config_Folder + "/" + file.Name()
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
func PersistWorkflowDocument(workflow XDWWorkflowDocument, workflowdef WorkflowDefinition) error {
	var err error
	var wfDoc []byte
	var wfDef []byte
	persistwf := dbint.Workflow{}
	persistwf.Created = util.Time_Now()
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
	existingwfs := dbint.Workflows{Action: cnst.SELECT}
	if err := dbint.NewDBEvent(&existingwfs); err != nil {
		log.Println(err.Error())
		return err
	}
	if existingwfs.Count > 0 {
		for k, exwf := range existingwfs.Workflows {
			if k > 0 {
				if exwf.XDW_Key == workflowdef.Ref+workflow.Patient.ID.Extension {
					wfStr := UpdateWorkflowStatus(exwf.XDW_Doc, "CLOSED")
					updtwfs := dbint.Workflows{Action: cnst.UPDATE}
					updtwf := dbint.Workflow{
						XDW_Key: exwf.XDW_Key,
						Version: exwf.Version,
						XDW_Doc: wfStr,
					}
					updtwfs.Workflows = append(updtwfs.Workflows, updtwf)
					if err := dbint.NewDBEvent(&updtwfs); err != nil {
						log.Println(err.Error())
						return err
					}
					log.Println("Closed existing workflow")
				}
			}
		}
	}
	persistwfs := dbint.Workflows{Action: cnst.INSERT}
	persistwfs.Workflows = append(persistwfs.Workflows, persistwf)
	if err = dbint.NewDBEvent(&persistwfs); err != nil {
		log.Println(err.Error())
	}
	return err
}
func UpdateWorkflowStatus(wfstr string, status string) string {
	wf := XDWWorkflowDocument{}
	if err := json.Unmarshal([]byte(wfstr), &wf); err != nil {
		log.Println(err.Error())
		return wfstr
	}
	wf.WorkflowStatus = status
	ret, err := json.Marshal(wf)
	if err != nil {
		log.Println(err.Error())
		return wfstr
	}
	return string(ret)
}
func GetActiveWorkflowEvents(pathway string, nhs string) (dbint.Events, error) {
	evs := dbint.Events{Action: cnst.SELECT}
	ev := dbint.Event{NhsId: nhs, Pathway: pathway, Version: "0"}
	evs.Events = append(evs.Events, ev)
	err := dbint.NewDBEvent(&evs)
	return evs, err
}
func Log(i interface{}) {
	util.Log(i)
}
func AWS_XDWs_API_Request(i *dbint.XDWS) error {
	log.Printf("Sending %s Request to %s", i.Action, TUK_DB_URL+cnst.XDWS)
	body, _ := json.Marshal(i)
	bodyBytes, err := newAWS_APIRequest(i.Action, cnst.XDWS, body)
	if err == nil {
		err = json.Unmarshal(bodyBytes, &i)
	}
	return err
}
func AWS_Workflows_API_Request(i *dbint.Workflows) error {
	log.Printf("Sending %s Request to %s", i.Action, TUK_DB_URL+cnst.WORKFLOWS)
	body, _ := json.Marshal(i)
	bodyBytes, err := newAWS_APIRequest(i.Action, cnst.WORKFLOWS, body)
	if err == nil {
		err = json.Unmarshal(bodyBytes, &i)
	}
	return err
}
func AWS_Subscriptions_API_Request(i *dbint.Subscriptions) error {
	log.Printf("Sending %s Request to %s", i.Action, TUK_DB_URL+cnst.SUBSCRIPTIONS)
	body, _ := json.Marshal(i)
	bodyBytes, err := newAWS_APIRequest(i.Action, cnst.SUBSCRIPTIONS, body)
	if err == nil {
		err = json.Unmarshal(bodyBytes, &i)
	}
	return err
}
func AWS_Events_API_Request(i *dbint.Events) error {
	log.Printf("Sending %s Request to %s", i.Action, TUK_DB_URL+cnst.EVENTS)
	body, _ := json.Marshal(i)
	bodyBytes, err := newAWS_APIRequest(i.Action, cnst.EVENTS, body)
	if err == nil {
		err = json.Unmarshal(bodyBytes, &i)
	}
	return err
}
func newAWS_APIRequest(act string, resource string, body []byte) ([]byte, error) {
	awsreq := tukhttp.AWS_APIRequest{
		URL:      TUK_DB_URL,
		Act:      act,
		Resource: resource,
		Timeout:  5,
		Body:     body,
	}
	err := tukhttp.NewRequest(&awsreq)
	return awsreq.Response, err
}

type eventsList []dbint.Event

func (e eventsList) Len() int {
	return len(e)
}
func (e eventsList) Less(i, j int) bool {
	return e[i].EventId > e[j].EventId
}
func (e eventsList) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
