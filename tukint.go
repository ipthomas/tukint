package tukint

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ipthomas/tukcnst"
	"github.com/ipthomas/tukdbint"
	"github.com/ipthomas/tukdsub"
	"github.com/ipthomas/tukpdq"
	"github.com/ipthomas/tukutil"
	"github.com/ipthomas/tukxdw"

	"github.com/aws/aws-lambda-go/events"
)

type EventServices struct {
	XDSRepService       ServiceState
	XDSRegService       ServiceState
	ODDService          ServiceState
	PDQv3Service        ServiceState
	PIXmService         ServiceState
	LoginService        ServiceState
	STSService          ServiceState
	SAMLService         ServiceState
	EventService        ServiceState
	BrokerService       ServiceState
	LogService          ServiceState
	DBService           ServiceState
	ServiceConfigs      []string
	WorkflowXDSConfigs  []tukxdw.XDSDocumentMeta
	ActivePathways      []string
	HTMLWidgets         []string
	XMLMessages         []string
	HTMLTemplates       *template.Template
	XMLTemplates        *template.Template
	WorkflowDefinitions []string
	WorkflowXDWMeta     []string
}
type ServiceState struct {
	Id             string `json:"id"`
	Desc           string `json:"desc"`
	Type           string `json:"type"`
	Proto          string `json:"proto"`
	Vers           string `json:"vers"`
	Enabled        bool   `json:"enabled"`
	Paused         bool   `json:"paused"`
	Debugmode      bool   `json:"debugmode"`
	Scheme         string `json:"scheme"`
	Host           string `json:"host"`
	Port           int    `json:"port"`
	Url            string `json:"url"`
	WSE            string `json:"wse"`
	DemoMode       bool   `json:"demomode"`
	XDSDomain      string `json:"xdsdomain"`
	User           string `json:"user"`
	Password       string `json:"password"`
	Org            string `json:"org"`
	Role           string `json:"role"`
	POU            string `json:"pou"`
	ClaimDialect   string `json:"claimdialect"`
	ClaimValue     string `json:"claimvalue"`
	RequestTmplt   string `json:"requesttmplt"`
	DataBase       string `json:"db"`
	TmpltsPath     string `json:"tmpltspath"`
	HTMLTmplts     string `json:"htmltmplts"`
	XMLTmplts      string `json:"xmltmplts"`
	BaseURLPath    string `json:"baseurlpath"`
	EventUrl       string `json:"eventurl"`
	FilesUrl       string `json:"filesurl"`
	XDWConfigsPath string `json:"xdwconfigspath"`
	FilesPath      string `json:"filespath"`
	Secret         string `json:"secret"`
	Token          string `json:"token"`
	CertPath       string `json:"certpath"`
	Certs          string `json:"certs"`
	Keys           string `json:"keys"`
	LogSrvc        string `json:"logsrvc"`
	DBSrvc         string `json:"dbsrvc"`
	BrokerSrvc     string `json:"brokersrvc"`
	STSSrvc        string `json:"stssrvc"`
	SAMLSrvc       string `json:"samlsrvc"`
	LoginSrvc      string `json:"loginsrvc"`
	PDQv3Srvc      string `json:"pdqv3srvc"`
	PIXmSrvc       string `json:"pixmsrvc"`
	ODDSrvc        string `json:"oddsrvc"`
	XDSRegSrvc     string `json:"xdsregsrvc"`
	XDSRepSrvc     string `json:"xdsrepsrvc"`
	CacheTimeout   int    `json:"cachetimeout"`
	CacheEnabled   bool   `json:"cacheenabled"`
	PatientSrvc    string `json:"patientsrvc"`
	TokenSrvc      string `json:"tokensrvc"`
	ContextTimeout int    `json:"contexttimeout"`
}
type TukEvent struct {
	Act                 string
	Task                string
	TaskID              int
	Status              string
	Op                  string
	Vers                int
	NHSId               string
	REGId               string
	REGOid              string
	PID                 string
	PIDOrg              string
	PIDOid              string
	GivenName           string
	FamilyName          string
	DOB                 string
	ZIP                 string
	Gender              string
	PatientIndependant  bool
	Notes               string
	Expression          string
	Topic               string
	Pathway             string
	Audience            string
	Include             string
	BrokerRef           string
	RowId               int64
	StateID             string
	SAML                string
	B64SAML             string
	ReturnJSON          bool
	ReturnXML           bool
	ReturnCode          int
	ContentType         string
	XDWDocuments        []tukdbint.Workflow
	Dashboard           tukxdw.Dashboard
	DBSubscriptions     tukdbint.Subscriptions
	DBEvents            []tukdbint.Event
	DBEvent             tukdbint.Event
	DBEventAcks         tukdbint.EventAcks
	PDQv3Response       tukpdq.PDQv3Response
	PatientXMLStr       string
	PIXmResponse        tukpdq.PIXmResponse
	HttpRequest         *http.Request
	HttpResponse        http.ResponseWriter
	HTTPMethod          string
	Body                string
	DocRef              string
	RepositoryUniqueId  string
	Base64EncodedFile   string
	EventServices       EventServices
	XDWWorkflowDocument tukxdw.XDWWorkflowDocument
	XDSDocumentMeta     tukxdw.XDSDocumentMeta
	WorkflowDefinition  tukxdw.WorkflowDefinition
	ConfigStr           string
}

var (
	Basepath   = os.Getenv(tukcnst.ENV_TUK_CONFIG)
	configFile = os.Getenv(tukcnst.ENV_TUK_CONFIG_FILE)
	LogFile    *os.File
	Regoid     = ""
	Services   = EventServices{}
	DebugMode  = true
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	Basepath = os.Getenv(tukcnst.ENV_TUK_CONFIG)
	configFile = os.Getenv(tukcnst.ENV_TUK_CONFIG_FILE)
	if Basepath == "" {
		Basepath = tukcnst.DEFAULT_TUK_BASEPATH
		log.Println("Environment Var 'TUK_CONFIG' not set")
	} else {
		if !strings.HasSuffix(Basepath, "/") {
			Basepath = Basepath + "/"
		}
	}
	log.Printf("Set BasePath = %s", Basepath)
	if configFile == "" {
		configFile = tukcnst.DEFAULT_TUK_SERVICE_CONFIG_FILE
		log.Println("Environment Var 'TUK_CONFIG_FILE' not set")
	} else {
		configFile = strings.TrimSuffix(configFile, ".json")
	}
	log.Printf("Set Config file = %s", configFile)
}
func InitTuki() error {
	var err error
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	lenabled, _ := strconv.ParseBool(os.Getenv("Log_Enabled"))
	if lenabled {
		LogFile = tukutil.CreateLog(tukcnst.DEFAULT_TUK_SERVICE_LOG_FOLDER)
	}
	if err = SetEventServiceState(); err == nil {
		err = cacheTemplates()
	}
	if err != nil {
		log.Println(err.Error())
		tukdbint.DBConn.Close()
		LogFile.Close()
		return err
	}
	Regoid = os.Getenv(tukcnst.ENV_REG_OID)
	if Regoid == "" {
		log.Printf("No Regional OID set in Environment Var %s. Checking for Event Service IDMapping", tukcnst.ENV_REG_OID)
		Regoid = tukdbint.GetIDMapsLocalId(tukcnst.XDSDOMAIN)
		log.Printf("IDMap Query returned %s", Regoid)
		if Regoid != tukcnst.XDSDOMAIN {
			log.Printf("Set Regional OID %s from Event Service Code System", Regoid)
		} else {
			log.Println("Warning. Unabable to obtain Regional OID")
		}
	} else {
		log.Printf("Set Regional OID %s from Environment Var %s", Regoid, tukcnst.ENV_REG_OID)
	}
	return nil
}
func InitTempFiles() error {
	statics := tukdbint.Statics{Action: tukcnst.SELECT}
	tukdbint.NewDBEvent(&statics)
	log.Printf("Loading %v static files to %s folder", statics.Count, os.TempDir())
	for k, static := range statics.Static {
		if k != 0 {
			err := tukutil.WriteFileToTempFolder(static.Content, os.TempDir()+"/"+static.Name)
			if err != nil {
				log.Println(err.Error())
			}
		}
	}
	return nil
}
func SetEventServiceState() error {
	log.Println("Initialising Service States")
	Services.ServiceConfigs = nil
	err := Services.SetEventServicesStates()
	hn, _ := os.Hostname()
	if hn != Services.EventService.Host {
		log.Printf("Warning: Configured Event Service Host Name %s is different to the OS Host Name %s. To be on safe side set the eventsrvc.json host value to equal the actual host name!", Services.EventService.Host, hn)
	} else {
		log.Printf("Configured Event Service Host Name %s is equal to the OS Host Name %s. So thats all good then!", Services.EventService.Host, hn)
	}
	return err
}
func cacheTemplates() error {
	var err error
	Services.XMLTemplates = template.New(tukcnst.XML)
	Services.HTMLTemplates = template.New(tukcnst.HTML)
	tmplts := tukdbint.Templates{Action: tukcnst.SELECT}
	tukdbint.NewDBEvent(&tmplts)
	log.Printf("loaded %v Templates", tmplts.Count)
	funcmap := getTemplateFuncMap()
	for _, tmplt := range tmplts.Templates {
		if tmplt.IsXML {
			Services.XMLTemplates, err = Services.XMLTemplates.New(tmplt.Name).Funcs(funcmap).Parse(tmplt.Template)
			Services.XMLMessages = append(Services.XMLMessages, tmplt.Name)
		} else {
			Services.HTMLTemplates, err = Services.HTMLTemplates.New(tmplt.Name).Funcs(funcmap).Parse(tmplt.Template)
			Services.HTMLWidgets = append(Services.HTMLWidgets, tmplt.Name)
		}
		if err != nil {
			return err
		}
	}
	sortTemplatesResponse()
	return nil
}
func getTemplateFuncMap() template.FuncMap {
	return template.FuncMap{
		"newuuid":          tukutil.NewUuid,
		"newid":            tukutil.Newid,
		"newzulu":          tukutil.Newzulu,
		"new30mfuturezulu": tukutil.New30mfutureyearzulu,
		"newdatetime":      tukutil.Newdatetime,
		"splitfhiroid":     tukutil.SplitFhirOid,
		"splitexpression":  tukutil.SplitExpression,
		"geticon":          tukutil.GetGlypicon,
	}
}
func (i *EventServices) SetEventServicesStates() error {
	var err error
	dbconn := tukdbint.TukDBConnection{DBUser: os.Getenv(tukcnst.ENV_DB_USER), DBPassword: os.Getenv(tukcnst.ENV_DB_PASSWORD), DBHost: os.Getenv(tukcnst.ENV_DB_HOST), DBPort: os.Getenv(tukcnst.ENV_DB_PORT), DBName: os.Getenv(tukcnst.ENV_DB_NAME)}
	if err := tukdbint.NewDBEvent(&dbconn); err != nil {
		log.Println(err.Error())
	}
	if err = i.loadServiceConfig(configFile); err != nil {
		log.Println(err.Error())
		return err
	}
	if err = i.loadServiceConfig(i.EventService.BrokerSrvc); err != nil {
		log.Println(err.Error())
	}
	if err = i.loadServiceConfig(i.EventService.PDQv3Srvc); err != nil {
		log.Println(err.Error())
	}
	if err = i.loadServiceConfig(i.EventService.PIXmSrvc); err != nil {
		log.Println(err.Error())
	}
	if err = i.loadServiceConfig(i.EventService.XDSRepSrvc); err != nil {
		log.Println(err.Error())
	}
	if err = i.loadServiceConfig(i.EventService.XDSRegSrvc); err != nil {
		log.Println(err.Error())
	}
	i.WorkflowDefinitions = tukxdw.GetWorkflowDefinitionNames()
	i.WorkflowXDWMeta = tukxdw.GetWorkflowXDSMetaNames()
	i.ActivePathways = tukxdw.GetActiveWorkflowNames()
	log.Printf("Initialised %v Event Services", len(i.ServiceConfigs))
	return err
}
func (i *EventServices) loadServiceConfig(srvc string) error {
	var err error
	var tuksrvcState = tukdbint.ServiceState{}
	srvc = strings.TrimSuffix(srvc, ".json")
	log.Printf("Loading Service Configuration %s", srvc)
	if tuksrvcState, err = tukdbint.GetServiceState(srvc); err == nil {
		srvcState := ServiceState{}
		if err := json.Unmarshal([]byte(tuksrvcState.Service), &srvcState); err != nil {
			log.Println(err.Error())
			return err
		}
		switch srvc {
		case configFile:
			i.EventService = srvcState
			i.EventService.setServiceWSE()
			DebugMode = i.EventService.Debugmode
			i.ServiceConfigs = append(i.ServiceConfigs, i.EventService.Id)
		case i.EventService.BrokerSrvc:
			i.BrokerService = srvcState
			i.BrokerService.setServiceWSE()
			i.ServiceConfigs = append(i.ServiceConfigs, i.BrokerService.Id)
		case i.EventService.PDQv3Srvc:
			i.PDQv3Service = srvcState
			i.PDQv3Service.setServiceWSE()
			i.ServiceConfigs = append(i.ServiceConfigs, i.PDQv3Service.Id)
		case i.EventService.PIXmSrvc:
			i.PIXmService = srvcState
			i.PIXmService.setServiceWSE()
			i.ServiceConfigs = append(i.ServiceConfigs, i.PIXmService.Id)
		case i.EventService.XDSRegSrvc:
			i.XDSRegService = srvcState
			i.XDSRegService.setServiceWSE()
			i.ServiceConfigs = append(i.ServiceConfigs, i.XDSRegService.Id)
		case i.EventService.XDSRepSrvc:
			i.XDSRepService = srvcState
			i.XDSRepService.setServiceWSE()
			i.ServiceConfigs = append(i.ServiceConfigs, i.XDSRepService.Id)
		}
		log.Println("Initialised " + srvcState.Desc + " State")
	}
	return err
}

func (i *TukEvent) HandleBrokerNotification() []byte {
	log.Println("Handling IHE DSUB Notification Message")
	event := tukdsub.DSUBEvent{
		BrokerURL:       i.EventServices.BrokerService.WSE,
		PDQ_SERVER_TYPE: i.EventServices.EventService.PatientSrvc,
		REG_OID:         Regoid,
		EventMessage:    i.Body,
	}
	switch event.PDQ_SERVER_TYPE {
	case tukcnst.PDQ_SERVER_TYPE_IHE_PDQV3:
		event.PDQ_SERVER_URL = i.EventServices.PDQv3Service.WSE
	case tukcnst.PDQ_SERVER_TYPE_IHE_PIXM:
		event.PDQ_SERVER_URL = i.EventServices.PIXmService.WSE
	}
	if err := tukdsub.New_Transaction(&event); err != nil {
		log.Println(err.Error())
	}
	log.Println("Sending Notification Message ACK to Broker")
	return []byte(tukcnst.GO_TEMPLATE_DSUB_ACK)
}
func (i *ServiceState) setServiceWSE() {
	if i.Id == os.Getenv(tukcnst.ENV_TUK_CONFIG_FILE) {
		i.WSE = i.Scheme + "://" + i.Host + ":" + tukutil.GetStringFromInt(i.Port) + "/" + i.BaseURLPath + "/" + i.EventUrl
	} else {
		i.WSE = i.Scheme + "://" + i.Host + ":" + tukutil.GetStringFromInt(i.Port) + "/" + i.Url
	}
	log.Printf("Set %s Event Service WSE %s", i.Desc, i.WSE)
}
func loadFile(file fs.DirEntry, folder string) []byte {
	var fileBytes []byte
	var err error
	fileBytes, err = os.ReadFile(folder + file.Name())
	if err != nil {
		log.Println(err.Error())
	} else {
		log.Printf("Loaded %s ", file.Name())
	}
	return fileBytes
}
func InitDatabase(mysqlFile string) {
	log.Println("Initialising Event Management Service Database")
	tukdbint.DBConn.Close()
	dbconn := tukdbint.TukDBConnection{DBUser: os.Getenv(tukcnst.ENV_DB_USER), DBPassword: os.Getenv(tukcnst.ENV_DB_PASSWORD), DBHost: os.Getenv(tukcnst.ENV_DB_HOST), DBPort: os.Getenv(tukcnst.ENV_DB_PORT), DBName: os.Getenv(tukcnst.ENV_DB_NAME)}
	if err := dbconn.InitialiseDatabase(Basepath + mysqlFile); err != nil {
		log.Println(err.Error())
		return
	}
}
func PersistServiceConfigs() {
	log.Println("Processing Event Service Config Files")
	if srvcs, err := tukutil.GetFolderFiles(Basepath + "services/"); err == nil {
		for _, file := range srvcs {
			if strings.HasSuffix(file.Name(), ".json") {
				if filebytes := loadFile(file, Basepath+"services/"); filebytes != nil {
					srvcs := tukdbint.ServiceStates{Action: tukcnst.DELETE}
					srvc := tukdbint.ServiceState{Name: strings.TrimSuffix(file.Name(), ".json")}
					srvcs.ServiceState = append(srvcs.ServiceState, srvc)
					tukdbint.NewDBEvent(&srvcs)
					srvcs = tukdbint.ServiceStates{Action: tukcnst.INSERT}
					srvc = tukdbint.ServiceState{Name: strings.TrimSuffix(file.Name(), ".json"), Service: string(filebytes)}
					srvcs.ServiceState = append(srvcs.ServiceState, srvc)
					tukdbint.NewDBEvent(&srvcs)
				}
			}
		}
	}
}
func PersistTemplates() {
	if xmlTmplts, err := tukutil.GetFolderFiles(Basepath + "templates/xml/"); err == nil {
		for _, file := range xmlTmplts {
			if strings.HasSuffix(file.Name(), ".xml") {
				filebytes := loadFile(file, Basepath+"templates/xml/")
				if filebytes != nil {
					log.Printf("Persisting XML Template %s", file.Name())
					tmplts := tukdbint.Templates{Action: tukcnst.DELETE}
					tmplt := tukdbint.Template{Name: strings.TrimSuffix(file.Name(), ".xml"), IsXML: true}
					tmplts.Templates = append(tmplts.Templates, tmplt)
					tukdbint.NewDBEvent(&tmplts)
					tmplts = tukdbint.Templates{Action: tukcnst.INSERT}
					tmplt = tukdbint.Template{Name: strings.TrimSuffix(file.Name(), ".xml"), IsXML: true, Template: string(filebytes)}
					tmplts.Templates = append(tmplts.Templates, tmplt)
					tukdbint.NewDBEvent(&tmplts)
				}
			}
		}
	}
	if htmlTmplts, err := tukutil.GetFolderFiles(Basepath + "templates/html/"); err == nil {
		for _, file := range htmlTmplts {
			if strings.HasSuffix(file.Name(), ".html") {
				filebytes := loadFile(file, Basepath+"templates/html/")
				if filebytes != nil {
					log.Printf("Persisting HTML Template %s", file.Name())
					tmplts := tukdbint.Templates{Action: tukcnst.DELETE}
					tmplt := tukdbint.Template{Name: strings.TrimSuffix(file.Name(), ".html")}
					tmplts.Templates = append(tmplts.Templates, tmplt)
					tukdbint.NewDBEvent(&tmplts)
					tmplts = tukdbint.Templates{Action: tukcnst.INSERT}
					tmplt = tukdbint.Template{Name: strings.TrimSuffix(file.Name(), ".html"), Template: string(filebytes)}
					tmplts.Templates = append(tmplts.Templates, tmplt)
					tukdbint.NewDBEvent(&tmplts)
				}
			}
		}
	}
}
func PersistXDWConfigs() {
	log.Println("Processing XDW Config Files")
	if xdwconfigs, err := tukutil.GetFolderFiles(Basepath + "xdwconfig/"); err == nil {
		for _, file := range xdwconfigs {
			splitname := strings.Split(file.Name(), ".")
			if len(splitname) < 2 {
				log.Printf("File %s is not a XDW Configuration File", file.Name())
				continue
			}
			suffix := strings.Split(file.Name(), ".")[1]
			if filebytes := loadFile(file, Basepath+"xdwconfig/"); filebytes != nil {
				trans := tukxdw.Transaction{
					Actor:            tukcnst.XDW_ADMIN_REGISTER_DEFINITION,
					Pathway:          splitname[0],
					DSUB_BrokerURL:   os.Getenv(tukcnst.DSUB_BROKER_URL),
					DSUB_ConsumerURL: os.Getenv(tukcnst.ENV_DSUB_CONSUMER_URL),
					Request:          filebytes,
				}
				if suffix == "_meta.json" {
					trans.Actor = tukcnst.XDW_ADMIN_REGISTER_XDS_META
				}
				tukxdw.Execute(&trans)
			} else {
				log.Printf("Unable to load file %s", file.Name())
			}
		}
	}
}
func (i *TukEvent) PrettyTime(time string) string {
	return strings.TrimSpace(strings.Split(strings.ReplaceAll(strings.ReplaceAll(time, "T", " "), "Z", ""), "+")[0])
}
func (i *TukEvent) IsBrokerExpression(expression string) bool {
	return tukutil.IsBrokerExpression(expression)
}
func (i *TukEvent) GetMappedId(lid string) string {
	return tukdbint.GetIDMapsMappedId(lid)
}
func (i *TukEvent) TaskNotes(task string) string {
	return tukxdw.GetTaskNotes(i.Pathway, i.NHSId, tukutil.GetIntFromString(task), i.Vers)
}
func (i *TukEvent) ElapsedTime() string {
	st := tukutil.GetTimeFromString(i.XDWWorkflowDocument.EffectiveTime.Value)
	duration := time.Since(st)
	log.Printf("Duration = %s", duration.String())
	elapsedTime := tukutil.PrettyPrintDuration(duration)
	log.Printf("Elapsed time for workflow %s nhs id %s version %v is %s", i.XDWWorkflowDocument.WorkflowDefinitionReference, i.XDWWorkflowDocument.Patient.ID.Extension, i.Vers, elapsedTime)
	return elapsedTime
}
func (i *TukEvent) LastUpdateTime() string {
	return i.PrettyTime(i.XDWWorkflowDocument.GetLatestWorkflowEventTime().String())
}
func (i *TukEvent) lastUpdateTime() time.Time {
	return i.XDWWorkflowDocument.GetLatestWorkflowEventTime()
}
func (i *TukEvent) TaskCompleteByTimeString(taskid string) string {
	log.Printf("Obtaining Workflow Task %s Complete By date", taskid)
	trans := tukxdw.Transaction{XDWDocument: i.XDWWorkflowDocument, XDWDefinition: i.WorkflowDefinition, Task_ID: tukutil.GetIntFromString(taskid)}
	return i.PrettyTime(trans.GetTaskCompleteByDate().String())
}
func (i *TukEvent) TaskDuration(taskid string) string {
	log.Printf("Obtaining task %s duration", taskid)
	trans := tukxdw.Transaction{XDWDefinition: i.WorkflowDefinition, XDWDocument: i.XDWWorkflowDocument, Task_ID: tukutil.GetIntFromString(taskid)}
	return trans.GetTaskDuration()
}
func (i *TukEvent) IsTaskOverdue(taskid string) bool {
	trans := tukxdw.Transaction{XDWDocument: i.XDWWorkflowDocument, XDWDefinition: i.WorkflowDefinition, Task_ID: tukutil.GetIntFromString(taskid)}
	return trans.IsTaskOverdue()
}
func (i *TukEvent) getWorkflowCompleteByDate() time.Time {
	log.Println("Obtaining Workflow Complete By Date")
	trans := tukxdw.Transaction{XDWDocument: i.XDWWorkflowDocument, XDWDefinition: i.WorkflowDefinition}
	return trans.GetWorkflowCompleteByDate()
}
func (i *TukEvent) CompletionTime() string {
	if i.WorkflowDefinition.CompleteByTime == "" {
		return "Non Specified"
	}
	return i.PrettyTime(i.getWorkflowCompleteByDate().String())
}
func (i *TukEvent) IsWorkflowOverdue() bool {
	if i.WorkflowDefinition.CompleteByTime == "" {
		return false
	}
	completionDate := i.getWorkflowCompleteByDate()
	if time.Now().Before(completionDate) {
		return false
	}
	if i.XDWWorkflowDocument.WorkflowStatus == tukcnst.CLOSED {
		lupdt := i.lastUpdateTime()
		if lupdt.Before(completionDate) {
			return false
		}
	}
	return true
}
func (i *TukEvent) WorkflowTimeRemaining() string {
	log.Printf("Obtaining time remaining for %s Workflow NHS ID %s", i.Pathway, i.NHSId)
	trans := tukxdw.Transaction{XDWDocument: i.XDWWorkflowDocument, XDWDefinition: i.WorkflowDefinition}
	return trans.GetWorkflowTimeRemaining()
}
func (i *TukEvent) parsePostEvent() []byte {
	i.HttpResponse.Header().Set(tukcnst.CONTENT_TYPE, tukcnst.SOAP_XML)
	i.ReturnXML = true
	i.HttpResponse.Header().Add(tukcnst.CONTENT_TYPE, tukcnst.APPLICATION_XML)
	b, _ := io.ReadAll(i.HttpRequest.Body)
	i.Body = string(b)
	return i.HandleBrokerNotification()
}
func (i *TukEvent) newXDWHandler() []byte {
	wfs := tukxdw.GetWorkflows(i.Pathway, i.NHSId, "", i.DocRef, i.Vers, false, i.Status)
	if wfs.Count == 1 {
		if err := xml.Unmarshal([]byte(wfs.Workflows[1].XDW_Doc), &i.XDWWorkflowDocument); err != nil {
			log.Println(err.Error())
			return nil
		}
		log.Println("Unmarshalled Workflow Document")
		if err := json.Unmarshal([]byte(wfs.Workflows[1].XDW_Def), &i.WorkflowDefinition); err != nil {
			log.Println(err.Error())
			return nil
		}
		log.Println("Unmarshalled Workflow Definition")
	}
	type apirsp struct {
		XDW tukxdw.XDWWorkflowDocument
		DEF tukxdw.WorkflowDefinition
	}
	a := apirsp{XDW: i.XDWWorkflowDocument, DEF: i.WorkflowDefinition}

	if i.ReturnJSON {
		if i.HttpResponse != nil {
			i.HttpResponse.Header().Add(tukcnst.CONTENT_TYPE, tukcnst.TEXT_PLAIN)
		}
		b, e := json.MarshalIndent(a, "", "  ")
		if e != nil {
			log.Println(e.Error())
			return []byte(e.Error())
		}
		return b
	}
	if i.ReturnXML {
		if i.HttpResponse != nil {
			i.HttpResponse.Header().Add(tukcnst.CONTENT_TYPE, tukcnst.TEXT_PLAIN)
		}
		b, err := xml.MarshalIndent(a, "", "  ")
		if err != nil {
			log.Println(err.Error())
			return []byte(err.Error())
		}
		return b
	}
	if i.TaskID > 0 {
		return i.WorkflowTasksWidget()
	}
	return i.XDWDocumentWidget()
}
func (i *TukEvent) newXDWSHandler() []byte {
	status := i.Status
	switch status {
	case tukcnst.TUK_STATUS_MET:
		status = tukcnst.TUK_STATUS_CLOSED
	case tukcnst.TUK_STATUS_MISSED:
		status = tukcnst.TUK_STATUS_CLOSED
	case tukcnst.TUK_STATUS_ESCALATED:
		status = tukcnst.TUK_STATUS_OPEN
	}
	if status != "" {
		log.Printf("Retrieving XDWS with Status %s", status)
	}
	wfs := tukdbint.GetWorkflows(i.Pathway, i.NHSId, "", i.DocRef, i.Vers, false, status)
	log.Printf("Total Workflow Count %v", wfs.Count)
	trans := tukxdw.Transaction{Workflows: wfs}
	trans.SetDashboardState()
	retwfs := trans.Workflows
	if i.Status == tukcnst.TUK_STATUS_MET {
		retwfs = trans.TargetMetWorkflows
	} else {
		if i.Status == tukcnst.TUK_STATUS_MISSED {
			retwfs = trans.OverdueWorkflows
		} else {
			if i.Status == tukcnst.TUK_STATUS_ESCALATED {
				retwfs = trans.EscalteWorkflows
			}
		}
	}
	for _, v := range retwfs.Workflows {
		if v.Id > 0 {
			i.XDWDocuments = append(i.XDWDocuments, v)
		}
	}
	if i.ReturnJSON {
		if i.HttpResponse != nil {
			i.HttpResponse.Header().Add(tukcnst.CONTENT_TYPE, tukcnst.TEXT_PLAIN)
		}
		b, e := json.MarshalIndent(i.XDWDocuments, "", "  ")
		if e != nil {
			log.Println(e.Error())
			return []byte(e.Error())
		}
		return b
	}
	if i.ReturnXML {
		if i.HttpResponse != nil {
			i.HttpResponse.Header().Add(tukcnst.CONTENT_TYPE, tukcnst.TEXT_PLAIN)
		}
		b, err := xml.MarshalIndent(i.XDWDocuments, "", "  ")
		if err != nil {
			log.Println(err.Error())
			return []byte(err.Error())
		}
		return b
	}
	return i.XDWDocumentsWidget()
}
func (i *TukEvent) manageSubscriptions() []byte {
	if i.Task == tukcnst.CANCEL && i.RowId != 0 {
		sub := tukdsub.DSUBEvent{Action: tukcnst.CANCEL, BrokerURL: i.EventServices.BrokerService.WSE, RowID: int64(i.RowId)}
		tukdsub.New_Transaction(&sub)
	}
	subs := tukdsub.DSUBEvent{Action: tukcnst.SELECT, Pathway: i.Pathway}
	tukdsub.New_Transaction(&subs)
	i.DBSubscriptions = subs.Subs
	if i.ReturnJSON {
		if i.HttpResponse != nil {
			i.HttpResponse.Header().Add(tukcnst.CONTENT_TYPE, tukcnst.APPLICATION_JSON)
		}
		jstr, err := json.Marshal(i.DBSubscriptions)
		if err != nil {
			return []byte(err.Error())
		}
		return jstr
	}
	return i.SubscriptionsWidget()
}
func (i *TukEvent) manageEvents() []byte {
	var rsp []byte
	switch i.Task {
	case tukcnst.CREATE:
		return i.createEvent()
	case tukcnst.LIST:
		evs := tukdbint.Events{Action: tukcnst.SELECT}
		ev := tukdbint.Event{Id: i.RowId, Pathway: i.Pathway, NhsId: i.NHSId, Version: i.Vers, TaskId: i.TaskID}
		evs.Events = append(evs.Events, ev)
		tukdbint.NewDBEvent(&evs)
		i.DBEvents = evs.Events
		if i.ReturnJSON {
			rsp, _ = json.MarshalIndent(evs, "", "  ")
		} else {
			rsp = i.eventsWidget()
		}
	}
	return rsp
}
func (i *TukEvent) createEvent() []byte {
	i.DBEvent = tukdbint.Event{}
	i.DBEvent.User = i.EventServices.EventService.User
	i.DBEvent.Org = i.EventServices.EventService.Org
	i.DBEvent.Role = i.EventServices.EventService.Role
	i.DBEvent.Pathway = i.Pathway
	i.DBEvent.NhsId = i.NHSId
	i.DBEvent.Topic = i.Topic
	i.DBEvent.Expression = i.Expression
	i.DBEvent.Comments = i.Notes
	i.DBEvent.Authors = i.EventServices.EventService.User + " " + i.EventServices.EventService.Org + " " + i.EventServices.EventService.Role
	i.DBEvent.ConfCode = i.Audience
	i.DBEvent.Version = i.Vers
	i.DBEvent.TaskId = i.TaskID
	evs := tukdbint.Events{Action: tukcnst.INSERT}
	evs.Events = append(evs.Events, i.DBEvent)
	err := tukdbint.NewDBEvent(&evs)
	if err != nil {
		log.Println(err.Error())
	} else {
		log.Printf("Persisted User Generated Event id %v for task %v pathway %s nhs id %v version %v", evs.LastInsertId, i.DBEvent.TaskId, i.DBEvent.Pathway, i.DBEvent.NhsId, i.Vers)
	}
	if err := tukxdw.ContentUpdater(i.Pathway, i.Vers, i.NHSId, i.EventServices.EventService.User); err != nil {
		log.Println(err.Error())
	}
	i.Act = tukcnst.WIDGET
	i.Task = tukcnst.XDW
	return i.handleRequest()
}
func (i *TukEvent) handleRequest() []byte {
	if i.Body == "" && i.HTTPMethod == http.MethodPost {
		log.Printf("Processing POST Request from %s", i.HttpRequest.RemoteAddr)
		defer i.HttpRequest.Body.Close()
		return i.parsePostEvent()
	}
	log.Printf("Processing GET %s %s Request from %s", i.Act, i.Task, i.EventServices.EventService.User)
	var rsp = []byte("ALIVE")
	switch i.Act {
	case tukcnst.PATIENT:
		rsp = i.queryPatient()
	case tukcnst.XDW_ACTOR_CONTENT_CONSUMER:
		rsp = i.xdwContentConsumer()
	case tukcnst.XDW_ACTOR_CONTENT_CREATOR:
		rsp = i.xdwContentCreator()
	case tukcnst.EVENTS:
		rsp = i.manageEvents()
	case tukcnst.SUBSCRIBER:
		rsp = i.manageSubscriptions()
	case tukcnst.SERVICES:
		rsp = i.manageServices()
	case tukcnst.WIDGET:
		rsp = i.GetWidget()
	case tukcnst.ADMIN:
		rsp = i.AdminSpaWidget()
	default:
		if i.DocRef != "" {
			filebytes, err := tukutil.GetFileBytes(os.TempDir() + "/" + i.DocRef)
			if err == nil {
				return filebytes
			}
		}
	}
	return rsp
}
func (i *TukEvent) xdwContentConsumer() []byte {
	i.ReturnXML = true
	trans := tukxdw.Transaction{
		Actor:              i.Act,
		User:               i.EventServices.EventService.User,
		Org:                i.EventServices.EventService.Org,
		Role:               i.EventServices.EventService.Role,
		Pathway:            i.Pathway,
		NHS_ID:             i.NHSId,
		Task_ID:            -1,
		XDWVersion:         -1,
		DSUB_BrokerURL:     os.Getenv(tukcnst.DSUB_BROKER_URL),
		DSUB_ConsumerURL:   os.Getenv(tukcnst.ENV_DSUB_CONSUMER_URL),
		Request:            []byte{},
		Response:           []byte{},
		Dashboard:          tukxdw.Dashboard{},
		XDWDefinition:      tukxdw.WorkflowDefinition{},
		XDSDocumentMeta:    tukxdw.XDSDocumentMeta{},
		XDWDocument:        tukxdw.XDWWorkflowDocument{},
		XDWState:           tukxdw.XDWState{},
		Workflows:          tukdbint.Workflows{},
		OpenWorkflows:      tukdbint.Workflows{},
		OverdueWorkflows:   tukdbint.Workflows{},
		EscalteWorkflows:   tukdbint.Workflows{},
		ClosedWorkflows:    tukdbint.Workflows{},
		TargetMetWorkflows: tukdbint.Workflows{},
		XDWEvents:          tukdbint.Events{},
		XDWTaskStates:      []tukxdw.XDWTaskState{},
	}
	if err := tukxdw.Execute(&trans); err != nil {
		log.Println(err.Error())
	}
	bytes, _ := xml.MarshalIndent(trans.XDWDocument, "", "  ")
	return bytes
}
func (i *TukEvent) xdwContentCreator() []byte {
	trans := tukxdw.Transaction{
		Actor:   tukcnst.XDW_ACTOR_CONTENT_CREATOR,
		Pathway: i.Pathway,
		NHS_ID:  i.NHSId,
		Request: []byte(i.Notes),
		User:    i.EventServices.EventService.User,
		Org:     i.EventServices.EventService.Org,
		Role:    i.EventServices.EventService.Role,
	}
	if err := tukxdw.Execute(&trans); err != nil {
		log.Println(err.Error())
	}
	if bytes, err := xml.MarshalIndent(trans.XDWDocument, "", "  "); err == nil {
		i.ConfigStr = string(bytes)
		return i.ConfigWidget()
	}
	i.Act = tukcnst.WIDGET
	i.Task = tukcnst.SPA
	return i.handleRequest()
}
func (i *TukEvent) manageServices() []byte {
	var err error
	var srvc = tukdbint.ServiceState{}
	var xdw = tukdbint.XDW{}
	var tmplt = tukdbint.Template{}
	switch i.Task {
	case tukcnst.TUK_TASK_RESTART:
		InitTuki()
		return []byte(tukcnst.OK)
	case tukcnst.TUK_TASK_GET:
		srvc, err = tukdbint.GetServiceState(i.Op)
		if err == nil {
			i.ConfigStr = srvc.Service
			if i.ReturnJSON {
				return []byte(srvc.Service)
			}
		}
		return i.ConfigWidget()
	case tukcnst.TUK_TASK_SET:
		if err = tukdbint.SetServiceState(i.Op, i.ConfigStr); err != nil {
			log.Println(err.Error())
		}
		i.Task = tukcnst.TUK_TASK_GET
		return i.manageServices()
	case tukcnst.TUK_TASK_GET_META:
		xdw, err = tukdbint.GetWorkflowXDSMeta(i.Op)
		if err != nil {
			log.Println(err.Error())
		}
		i.ConfigStr = xdw.XDW
		if i.ReturnJSON {
			return []byte(xdw.XDW)
		}
		return i.ConfigWidget()
	case tukcnst.TUK_TASK_SET_META:
		if err = tukdbint.SetWorkflowDefinition(i.Op, i.ConfigStr, true); err != nil {
			log.Println(err.Error())
		}
		i.Task = tukcnst.TUK_TASK_GET_META
		return i.manageServices()
	case tukcnst.TUK_TASK_GET_XDW:
		xdw, err = tukdbint.GetWorkflowDefinition(i.Op)
		if err != nil {
			log.Println(err.Error())
		}
		i.ConfigStr = xdw.XDW
		if i.ReturnJSON {
			return []byte(xdw.XDW)
		}
		return i.ConfigWidget()
	case tukcnst.TUK_TASK_SET_XDW:
		if err = tukdbint.SetWorkflowDefinition(i.Op, i.ConfigStr, false); err != nil {
			log.Println(err.Error())
		}
		i.Task = tukcnst.TUK_TASK_GET_XDW
		return i.manageServices()
	case tukcnst.TUK_TASK_GET_HTML:
		tmplt, err = tukdbint.GetTemplate(i.Op, false)
		if err != nil {
			log.Println(err.Error())
		}
		i.ConfigStr = tmplt.Template
		return i.ConfigWidget()
	case tukcnst.TUK_TASK_SET_HTML:
		if err := tukdbint.SetTemplate(i.Op, false, i.ConfigStr); err != nil {
			log.Println(err.Error())
		}
		i.Task = tukcnst.TUK_TASK_GET_HTML
		return i.manageServices()
	case tukcnst.TUK_TASK_GET_XML:
		tmplt, err = tukdbint.GetTemplate(i.Op, true)
		if err != nil {
			log.Println(err.Error())
		}
		i.ConfigStr = tmplt.Template
		return i.ConfigWidget()
	case tukcnst.TUK_TASK_SET_XML:
		if err := tukdbint.SetTemplate(i.Op, true, i.ConfigStr); err != nil {
			log.Println(err.Error())
		}
		i.Task = tukcnst.TUK_TASK_GET_XML
		return i.manageServices()
	}
	return nil
}
func (i *TukEvent) queryPatient() []byte {
	if err := i.setPatientInfo(); err != nil {
		log.Println(err.Error())
		return []byte("Patient Service is currently unavailable. Please try later")
	}
	return i.PatientWidget()
}
func (i *TukEvent) setPatientInfo() error {
	var url = ""
	cache := false
	switch i.EventServices.EventService.PatientSrvc {
	case tukcnst.PDQ_SERVER_TYPE_IHE_PIXM:
		url = i.EventServices.PIXmService.WSE
		cache = i.EventServices.PIXmService.CacheEnabled
	case tukcnst.PDQ_SERVER_TYPE_IHE_PDQV3:
		url = i.EventServices.PDQv3Service.WSE
		cache = i.EventServices.PDQv3Service.CacheEnabled
	}
	pdq := tukpdq.PDQQuery{
		Server_Mode: i.EventServices.EventService.PatientSrvc,
		Server_URL:  url,
		NHS_ID:      i.NHSId,
		MRN_ID:      i.PID,
		MRN_OID:     i.PIDOid,
		REG_ID:      i.REGId,
		REG_OID:     i.REGOid,
		Cache:       cache,
	}
	log.Printf("Sending %s PDQ request to %s", i.EventServices.EventService.PatientSrvc, url)
	if err := tukpdq.New_Transaction(&pdq); err == nil {
		switch i.EventServices.EventService.PatientSrvc {
		case tukcnst.PDQ_SERVER_TYPE_IHE_PIXM:
			if err := json.Unmarshal(pdq.Response, &i.PIXmResponse); err != nil {
				log.Println(err.Error())
				return err
			}
			i.FamilyName = pdq.FamilyName
			i.GivenName = pdq.GivenName
			i.DOB = pdq.BirthDate
			i.ZIP = pdq.Zip
			i.Gender = pdq.Gender
		case tukcnst.PDQ_SERVER_TYPE_IHE_PDQV3:
			if err := xml.Unmarshal(pdq.Response, &i.PDQv3Response); err != nil {
				log.Println(err.Error())
				return err
			}
		}
		i.REGOid = os.Getenv(tukcnst.ENV_REG_OID)
		i.NHSId = pdq.NHS_ID
		i.REGId = pdq.REG_ID
		i.PID = pdq.MRN_ID
		i.PIDOid = pdq.MRN_OID
		return nil
	} else {
		return err
	}
}

// sort interfaces

type widgets []string
type xmlmsgs []string

func sortTemplatesResponse() {
	w := widgets{}
	w = append(w, Services.HTMLWidgets...)
	sort.Sort(w)
	Services.HTMLWidgets = w
	x := xmlmsgs{}
	x = append(x, Services.XMLMessages...)
	sort.Sort(x)
	Services.XMLMessages = x
}
func (e widgets) Len() int {
	return len(e)
}
func (e widgets) Less(i, j int) bool {
	return e[i] < e[j]
}
func (e widgets) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
func (e xmlmsgs) Len() int {
	return len(e)
}
func (e xmlmsgs) Less(i, j int) bool {
	return e[i] < e[j]
}
func (e xmlmsgs) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// Server

func TukEventServer() {
	debugMode := Services.EventService.Debugmode
	log.Printf("Event Service set to Debug Mode : %v", debugMode)
	demoMode := Services.EventService.DemoMode
	log.Printf("Event Service set to Demo Mode : %v", demoMode)
	isSecure := Services.EventService.Scheme == "https"
	log.Printf("Event Service set to Secure Mode : %v", isSecure)
	http.HandleFunc("/"+Services.EventService.BaseURLPath+"/"+Services.EventService.EventUrl, tukutil.WriteResponseHeaders(Handle_TUK_HTTP_Request, isSecure))
	http.Handle("/"+Services.EventService.FilesUrl, http.StripPrefix("/"+Services.EventService.FilesUrl, http.FileServer(http.Dir(Basepath+"/"+Services.EventService.FilesPath))))
	http.Handle(Services.EventService.BaseURLPath, http.StripPrefix(Services.EventService.BaseURLPath+Services.EventService.FilesUrl, http.FileServer(http.Dir(Basepath+"/"+Services.EventService.FilesPath))))
	log.Println("Inialised Event Management Handler - " + Services.EventService.WSE)

	monitorApp()
	log.Println("Initialised Application Monitor")
	startUpMessage()
	if isSecure {
		log.Fatal(http.ListenAndServeTLS(":"+strconv.Itoa(Services.EventService.Port), Basepath+Services.EventService.CertPath+"/"+Services.EventService.Certs, Basepath+Services.EventService.CertPath+"/"+Services.EventService.Keys, nil))
	} else {
		log.Fatal(http.ListenAndServe(":"+strconv.Itoa(Services.EventService.Port), nil))
	}
}
func startUpMessage() {
	log.Println("Starting " + Services.EventService.Desc)
	log.Println("Listening for Notifications on " + Services.EventService.WSE + "eventservice/event")
	log.Println("Event Manager Swagger API. " + Services.EventService.WSE)
	log.Println("Event Manager Admin GUI. " + Services.EventService.WSE + "eventservice/event?act=admin&user=test&org=spirit&role=admin")
}
func monitorApp() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		signalType := <-ch
		signal.Stop(ch)
		log.Println("")
		log.Println("*********************************")
		log.Println("Exit command received. Exiting...")
		switch signalType {
		case os.Interrupt:
			log.Println("FATAL: CTRL+C pressed")
		case syscall.SIGTERM:
			log.Println("FATAL: SIGTERM detected")
		}
		tukdbint.DBConn.Close()
		log.Println("Closed DB connection")
		LogFile.Close()
		os.Exit(1)
	}()
}
func (i *TukEvent) setAwsResponseHeaders() map[string]string {
	awsHeaders := make(map[string]string)
	awsHeaders["Server"] = "Tiani_Spirit_UK"
	if i.ReturnJSON {
		awsHeaders[tukcnst.CONTENT_TYPE] = tukcnst.APPLICATION_JSON
	} else {
		if i.ReturnXML {
			awsHeaders[tukcnst.CONTENT_TYPE] = tukcnst.APPLICATION_XML
		} else {
			awsHeaders[tukcnst.CONTENT_TYPE] = tukcnst.TEXT_HTML
		}
	}
	awsHeaders["Access-Control-Allow-Origin"] = "*"
	awsHeaders["Access-Control-Allow-Headers"] = "accept, Content-Type"
	awsHeaders["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
	if Services.EventService.Scheme == "https" {
		awsHeaders["Strict-Transport-Security"] = "max-age=31536000"
	}
	return awsHeaders
}

func Handle_AWS_API_GW_Request(request events.APIGatewayProxyRequest) (*events.APIGatewayProxyResponse, error) {
	if strings.HasPrefix(request.Path, "/tmp/") {
		i := TukEvent{}
		if filebyte, err := tukutil.GetFileBytes(request.Path); err == nil {
			return &events.APIGatewayProxyResponse{
				StatusCode: http.StatusOK,
				Headers:    i.setAwsResponseHeaders(),
				Body:       string(filebyte),
			}, nil
		}
	}
	i := TukEvent{REGOid: Regoid, EventServices: Services}
	i.HTTPMethod = request.HTTPMethod
	i.Body = request.Body
	i.Audience = "N"
	i.ReturnCode = 200
	log.Printf("Processing request data for request %s.\n", request.Path)
	log.Printf("Body size = %d.\n", len(request.Body))
	log.Println("Headers:")
	for key, value := range request.Headers {
		fmt.Printf("    %s: %s\n", key, value)
		if key == tukcnst.ACCEPT && value == tukcnst.APPLICATION_JSON {
			i.ReturnJSON = true
		}
		if key == tukcnst.ACCEPT && value == tukcnst.APPLICATION_XML {
			i.ReturnXML = true
		}
		if key == tukcnst.CONTENT_TYPE {
			i.ContentType = value
		}
		if key == tukcnst.AUTHORIZATION {
			if strings.HasPrefix(value, "Basic ") {
				i.SAML = strings.TrimPrefix(value, "Basic ")
			} else {
				i.SAML = value
			}
		}
	}
	log.Println("AWS API Query Parameters")
	for key, value := range request.QueryStringParameters {
		log.Printf("    %s: %s\n", key, value)
		switch key {
		case tukcnst.TUK_EVENT_QUERY_PARAM_SAML:
			i.SAML = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_ACT:
			i.Act = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_TASK:
			i.Task = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_OP:
			i.Op = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_STATUS:
			i.Status = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_ID:
			if value != "" {
				_, err := strconv.ParseInt(value, 0, 0)
				if err != nil {
					i.RowId = 0
				} else {
					i.RowId = int64(tukutil.GetIntFromString(value))
				}
			}
		case tukcnst.TUK_EVENT_QUERY_PARAM_TASK_ID:
			if value == "" {
				i.TaskID = -1
			} else {
				i.TaskID = tukutil.GetIntFromString(value)
			}
		case tukcnst.TUK_EVENT_QUERY_PARAM_VERSION:
			if value == "" {
				i.Vers = -1
			} else {
				i.Vers = tukutil.GetIntFromString(value)
			}
		case tukcnst.TUK_EVENT_QUERY_PARAM_ROLE:
			i.EventServices.EventService.Role = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_USER:
			i.EventServices.EventService.User = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_ORG:
			i.EventServices.EventService.Org = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_PATHWAY:
			i.Pathway = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_TOPIC:
			i.Topic = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_EXPRESSION:
			i.Expression = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_NOTES:
			i.Notes = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_NHS:
			i.NHSId = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_AUDIEANCE:
			i.Audience = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_CONFIG:
			i.ConfigStr = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_DOCREF:
			i.DocRef = value
		case tukcnst.TUK_EVENT_QUERY_PARAM_FORMAT:
			switch value {
			case tukcnst.XML:
				i.ReturnXML = true
			case tukcnst.JSON:
				i.ReturnJSON = true
			}
		}
	}
	tukrsp := i.handleRequest()
	awsHeaders := make(map[string]string)
	if i.ReturnJSON {
		awsHeaders[tukcnst.CONTENT_TYPE] = tukcnst.APPLICATION_JSON
	}
	if i.ReturnXML {
		awsHeaders[tukcnst.CONTENT_TYPE] = tukcnst.APPLICATION_XML
	}
	return &events.APIGatewayProxyResponse{
		StatusCode: i.ReturnCode,
		Headers:    i.setAwsResponseHeaders(),
		Body:       string(tukrsp),
	}, nil
}
func Handle_TUK_HTTP_Request(rsp http.ResponseWriter, req *http.Request) {
	log.Printf("Received http %s request from %s. Processing New Event", req.Method, req.RemoteAddr)
	i := TukEvent{REGOid: Regoid, EventServices: Services, HttpRequest: req, HttpResponse: rsp}
	req.ParseForm()
	i.EventServices.EventService.User = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_USER)
	i.EventServices.EventService.Org = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_ORG)
	i.EventServices.EventService.Role = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_ROLE)
	i.Act = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_ACT)
	i.Task = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_TASK)
	i.Status = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_STATUS)
	i.Op = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_OP)
	i.NHSId = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_NHS)
	i.Audience = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_AUDIEANCE)
	i.Pathway = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_PATHWAY)
	i.Topic = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_TOPIC)
	i.Expression = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_EXPRESSION)
	i.Notes = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_NOTES)
	i.ConfigStr = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_CONFIG)
	i.DocRef = req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_DOCREF)
	if req.Header.Get(tukcnst.ACCEPT) == tukcnst.APPLICATION_JSON || req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_FORMAT) == tukcnst.JSON {
		i.ReturnJSON = true
	}
	if req.Header.Get(tukcnst.ACCEPT) == tukcnst.APPLICATION_XML || req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_FORMAT) == tukcnst.XML {
		i.ReturnXML = true
	}
	if i.Audience == "" {
		i.Audience = "N"
	}
	if req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_ID) != "" {
		i.RowId = int64(tukutil.GetIntFromString(req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_ID)))
	} else {
		i.RowId = 0
	}
	if req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_VERSION) != "" {
		i.Vers = tukutil.GetIntFromString(req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_VERSION))
	} else {
		i.Vers = -1
	}
	if req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_TASK_ID) != "" {
		i.TaskID = tukutil.GetIntFromString(req.FormValue(tukcnst.TUK_EVENT_QUERY_PARAM_TASK_ID))
	} else {
		i.TaskID = -1
	}
	i.HTTPMethod = req.Method
	i.printFormValues()

	i.HttpResponse.Write(i.handleRequest())
}
func (i *TukEvent) printFormValues() {
	if DebugMode {
		for key, values := range i.HttpRequest.Form {
			log.Println("Key : " + key + " Value : " + values[0])
		}
	}
}

// Widgets

func (i *TukEvent) GetWidget() []byte {
	log.Printf("Processing %s Widget Request", i.Task)
	switch i.Task {
	case tukcnst.SPA:
		return i.UserSpaWidget()
	case tukcnst.DASHBOARD:
		return i.DashboardWidget()
	case tukcnst.PATIENT:
		return i.PatientXDWs()
	case tukcnst.TIMELINE:
		return i.TimelineWidget()
	case tukcnst.XDW:
		return i.newXDWHandler()
	case tukcnst.XDWS:
		return i.newXDWSHandler()
	case tukcnst.CONFIG:
		return i.ConfigWidget()
	}
	return []byte("invalid widget request")
}
func (i *TukEvent) PatientXDWs() []byte {
	i.setPatientInfo()
	i.Task = tukcnst.XDWS
	return i.handleRequest()
}
func (i *TukEvent) PatientWidget() []byte {
	var b bytes.Buffer
	err := i.EventServices.HTMLTemplates.ExecuteTemplate(&b, "pixmpatient", i)
	if err != nil {
		log.Println(err.Error())
		return []byte(err.Error())
	}
	return b.Bytes()
}
func (i *TukEvent) XDWDocumentWidget() []byte {
	var b bytes.Buffer
	err := i.EventServices.HTMLTemplates.ExecuteTemplate(&b, tukcnst.TUK_TEMPLATE_WORKFLOW, i)
	if err != nil {
		log.Println(err.Error())
		return []byte(err.Error())
	}
	return b.Bytes()
}
func (i *TukEvent) WorkflowTasksWidget() []byte {
	var b bytes.Buffer
	err := i.EventServices.HTMLTemplates.ExecuteTemplate(&b, tukcnst.TUK_TEMPLATE_WORKFLOW_TASKS, i)
	if err != nil {
		log.Println(err.Error())
		return []byte(err.Error())
	}
	return b.Bytes()
}
func (i *TukEvent) DashboardWidget() []byte {
	switch strings.ToUpper(i.Status) {
	case tukcnst.OPEN:
		i.Status = tukcnst.OPEN
	case tukcnst.CLOSED:
		i.Status = tukcnst.TUK_STATUS_CLOSED
	case tukcnst.TUK_STATUS_MET:
		i.Status = tukcnst.TUK_STATUS_CLOSED
	case tukcnst.TUK_STATUS_MISSED:
		i.Status = tukcnst.TUK_STATUS_CLOSED
	case tukcnst.TUK_STATUS_ESCALATED:
		i.Status = tukcnst.OPEN
	}
	trans := tukxdw.Transaction{Workflows: tukxdw.GetWorkflows(i.Pathway, i.NHSId, "", i.DocRef, i.Vers, false, "")}
	trans.SetDashboardState()
	if i.Status == tukcnst.TUK_STATUS_MET {
		trans := tukxdw.Transaction{Workflows: trans.TargetMetWorkflows}
		trans.SetDashboardState()
	} else {
		if i.Status == tukcnst.TUK_STATUS_MISSED {
			trans := tukxdw.Transaction{Workflows: trans.OverdueWorkflows}
			trans.SetDashboardState()
		} else {
			if i.Status == tukcnst.TUK_STATUS_ESCALATED {
				trans := tukxdw.Transaction{Workflows: trans.EscalteWorkflows}
				trans.SetDashboardState()
			}
		}
	}
	i.Dashboard = trans.Dashboard
	var err error
	var tplReturn bytes.Buffer
	if err = i.EventServices.HTMLTemplates.ExecuteTemplate(&tplReturn, tukcnst.TUK_TEMPLATE_DASHBOARD_WIDGET, i); err != nil {
		log.Println(err.Error())
	}
	return tplReturn.Bytes()
}
func (i *TukEvent) TimelineWidget() []byte {
	i.Task = tukcnst.XDW
	i.newXDWSHandler()
	var err error
	var tplReturn bytes.Buffer
	if err = i.EventServices.HTMLTemplates.ExecuteTemplate(&tplReturn, tukcnst.TUK_TEMPLATE_TIMELINE_WIDGET, i); err != nil {
		log.Println(err.Error())
	}
	return tplReturn.Bytes()
}
func (i *TukEvent) AdminSpaWidget() []byte {
	i.EventServices.ActivePathways = tukxdw.GetActiveWorkflowNames()
	var tplReturn bytes.Buffer
	switch i.Task {
	case tukcnst.TUK_TASK_RESTART:
		InitTuki()
	case tukcnst.TUK_TASK_INIT_SERVICES:
		PersistServiceConfigs()
		InitTuki()
	case tukcnst.TUK_TASK_INIT_TEMPLATES:
		PersistTemplates()
		InitTuki()
	case tukcnst.TUK_TASK_INIT_XDWS:
		PersistXDWConfigs()
		InitTuki()
	}
	if err := i.EventServices.HTMLTemplates.ExecuteTemplate(&tplReturn, tukcnst.TUK_TEMPLATE_ADMIN_SPA_WIDGET, i); err != nil {
		log.Println(err.Error())
		return []byte(err.Error())
	}
	return tplReturn.Bytes()
}
func (i *TukEvent) UserSpaWidget() []byte {
	i.EventServices.ActivePathways = tukxdw.GetActiveWorkflowNames()
	var tplReturn bytes.Buffer
	if err := i.EventServices.HTMLTemplates.ExecuteTemplate(&tplReturn, tukcnst.TUK_TEMPLATE_SPA_WIDGET, i); err != nil {
		log.Println(err.Error())
		return []byte(err.Error())
	}
	return tplReturn.Bytes()
}
func (i *TukEvent) eventsWidget() []byte {
	var tplReturn bytes.Buffer
	if err := i.EventServices.HTMLTemplates.ExecuteTemplate(&tplReturn, tukcnst.TUK_TEMPLATE_EVENTS_WIDGET, i); err != nil {
		log.Println(err.Error())
		return []byte(err.Error())
	}
	return tplReturn.Bytes()
}
func (i *TukEvent) ConfigWidget() []byte {
	var b bytes.Buffer
	err := i.EventServices.HTMLTemplates.ExecuteTemplate(&b, tukcnst.TUK_TEMPLATE_CONFIG_WIDGET, i)
	if err != nil {
		log.Println(err.Error())
		return []byte(err.Error())
	}
	return b.Bytes()
}
func (i *TukEvent) XDWDocumentsWidget() []byte {
	var b bytes.Buffer
	err := i.EventServices.HTMLTemplates.ExecuteTemplate(&b, tukcnst.TUK_TEMPLATE_WORKFLOWS_WIDGET, i)
	if err != nil {
		log.Println(err.Error())
		return []byte(err.Error())
	}
	return b.Bytes()
}
func (i *TukEvent) SubscriptionsWidget() []byte {
	var tplReturn bytes.Buffer
	if err := i.EventServices.HTMLTemplates.ExecuteTemplate(&tplReturn, tukcnst.TUK_TEMPLATE_SUBSCRIPTIONS_WIDGET, i); err != nil {
		log.Println(err.Error())
		return []byte(err.Error())
	}
	return tplReturn.Bytes()
}
