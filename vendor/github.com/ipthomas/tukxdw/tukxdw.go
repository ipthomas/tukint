package tukxdw

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ipthomas/tukcnst"
	"github.com/ipthomas/tukdbint"
	"github.com/ipthomas/tukdsub"
	"github.com/ipthomas/tukutil"
)

var DebugMode = true
var Regional_OID = os.Getenv(tukcnst.ENV_REG_OID)

type Interface interface {
	execute() error
}
type Transaction struct {
	Actor            string
	User             string
	Org              string
	Role             string
	Pathway          string
	Expression       string
	NHS_ID           string
	Task_ID          int
	XDWVersion       int
	DSUB_BrokerURL   string
	DSUB_ConsumerURL string
	Request          []byte
	Response         []byte
	Dashboard        Dashboard
	XDWDefinition    WorkflowDefinition
	XDSDocumentMeta  XDSDocumentMeta
	XDWDocument      XDWWorkflowDocument
	XDWState         tukdbint.WorkflowStates
	Workflows        tukdbint.Workflows
	XDWEvents        tukdbint.Events
}
type XDWTaskState struct {
	TaskID              int
	Created             string
	CompleteBy          string
	Status              string
	IsOverdue           bool
	LatestTaskEventTime time.Time
	TaskDuration        time.Duration
	PrettyTaskDuration  string
}
type Dashboard struct {
	Total        int
	InProgress   int
	TargetMet    int
	TargetMissed int
	Escalated    int
	Complete     int
}

type XDSDocumentMeta struct {
	ID                    string `json:"id"`
	Repositoryuniqueid    string `json:"repositoryuniqueid"`
	Registryoid           string `json:"registryoid"`
	Languagecode          string `json:"languagecode"`
	Docname               string `json:"docname"`
	Docdesc               string `json:"docdesc"`
	DocID                 string `json:"docid"`
	Authorinstitution     string `json:"authorinstitution"`
	Authorperson          string `json:"authorperson"`
	Classcode             string `json:"classcode"`
	Classcodescheme       string `json:"classcodescheme"`
	Classcodevalue        string `json:"classcodevalue"`
	Typecode              string `json:"typecode"`
	Typecodescheme        string `json:"typecodescheme"`
	Typecodevalue         string `json:"typecodevalue"`
	Practicesettingcode   string `json:"practicesettingcode"`
	Practicesettingscheme string `json:"practicesettingscheme"`
	Practicesettingvalue  string `json:"practicesettingvalue"`
	Confcode              string `json:"confcode"`
	Confcodescheme        string `json:"confcodescheme"`
	Confcodevalue         string `json:"confcodevalue"`
	Facilitycode          string `json:"facilitycode"`
	Facilitycodescheme    string `json:"facilitycodescheme"`
	Facilitycodevalue     string `json:"facilitycodevalue"`
	Formatcode            string `json:"formatcode"`
	Formatcodescheme      string `json:"formatcodescheme"`
	Formatcodevalue       string `json:"formatcodevalue"`
	Mimetype              string `json:"mimetype"`
	Objecttype            string `json:"objecttype"`
}
type WorkflowDefinition struct {
	Ref                 string `json:"ref"`
	Name                string `json:"name"`
	Confidentialitycode string `json:"confidentialitycode"`
	StartByTime         string `json:"startbytime"`
	CompleteByTime      string `json:"completebytime"`
	ExpirationTime      string `json:"expirationtime"`
	CompletionBehavior  []struct {
		Completion struct {
			Condition string `json:"condition"`
		} `json:"completion"`
	} `json:"completionBehavior"`
	Tasks []struct {
		ID              string `json:"id"`
		Tasktype        string `json:"tasktype"`
		Name            string `json:"name"`
		Description     string `json:"description"`
		ActualOwner     string `json:"actualowner"`
		ExpirationTime  string `json:"expirationtime,omitempty"`
		StartByTime     string `json:"startbytime,omitempty"`
		CompleteByTime  string `json:"completebytime"`
		IsSkipable      bool   `json:"isskipable,omitempty"`
		PotentialOwners []struct {
			OrganizationalEntity struct {
				User string `json:"user"`
			} `json:"organizationalEntity"`
		} `json:"potentialOwners,omitempty"`
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
	ActivationTime        string `xml:"activationTime"`
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

// sort interface for Document Events
type DocumentEventsList []DocumentEvent

func (e DocumentEventsList) Len() int {
	return len(e)
}
func (e DocumentEventsList) Less(i, j int) bool {
	return e[i].EventTime > e[j].EventTime
}
func (e DocumentEventsList) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func Execute(i Interface) error {
	return i.execute()
}

// IHE XDW Actors

func (i *Transaction) execute() error {
	switch i.Actor {
	case tukcnst.XDW_ADMIN_REGISTER_DEFINITION:
		return i.RegisterWorkflowDefinition(false)
	case tukcnst.XDW_ADMIN_REGISTER_XDS_META:
		return i.RegisterWorkflowDefinition(true)
	case tukcnst.XDW_ACTOR_CONTENT_CREATOR:
		return i.ContentCreator()
	case tukcnst.XDW_ACTOR_CONTENT_CONSUMER:
		return i.ContentConsumer()
	case tukcnst.XDW_ACTOR_CONTENT_UPDATER:
		return i.ContentUpdater()
	}
	return nil
}

// IHE XDW Content Updater

func (i *Transaction) ContentUpdater() error {
	i.Workflows = tukdbint.Workflows{Action: tukcnst.SELECT}
	wf := tukdbint.Workflow{Pathway: i.Pathway, NHSId: i.NHS_ID, Version: i.XDWVersion}
	i.Workflows.Workflows = append(i.Workflows.Workflows, wf)
	if err := tukdbint.NewDBEvent(&i.Workflows); err != nil {
		log.Println(err.Error())
		return err
	}
	i.XDWEvents = tukdbint.Events{Action: tukcnst.SELECT}
	ev := tukdbint.Event{Pathway: i.Pathway, NhsId: i.NHS_ID, Version: i.XDWVersion}
	i.XDWEvents.Events = append(i.XDWEvents.Events, ev)
	log.Printf("Updating State for %v Workflows", i.Workflows.Count)
	if err := tukdbint.NewDBEvent(&i.Workflows); err != nil {
		log.Println(err.Error())
		return err
	}
	for _, wf := range i.Workflows.Workflows {
		log.Printf("Updating %s Workflow Version %v for NHS ID %s", wf.Pathway, wf.Version, wf.NHSId)
		if wf.Id == 0 {
			continue
		}
		if err := json.Unmarshal([]byte(wf.XDW_Def), &i.XDWDefinition); err != nil {
			log.Println(err.Error())
			return err
		}
		if err := xml.Unmarshal([]byte(wf.XDW_Doc), &i.XDWDocument); err != nil {
			log.Println(err.Error())
			return err
		}
		log.Printf("Processing %v Events", i.XDWEvents.Count)
		newEvents := tukdbint.Events{}
		for _, ev := range i.XDWEvents.Events {
			if ev.Id != 0 && ev.Pathway == wf.Pathway && ev.NhsId == wf.NHSId && ev.Version == wf.Version {
				log.Printf("Processing Event ID %v Obtaining Workflow Task %v", ev.Id, ev.TaskId)
				for _, task := range i.XDWDocument.TaskList.XDWTask {
					if task.TaskData.TaskDetails.ID == tukutil.GetStringFromInt(ev.TaskId) {
						log.Printf("Found Task %s Searching Task Events for matching event ID %v", task.TaskData.TaskDetails.ID, ev.Id)
						hasevent := false
						for _, taskevent := range task.TaskEventHistory.TaskEvent {
							if taskevent.ID == tukutil.GetStringFromInt(int(ev.Id)) {
								log.Printf("Task %s Event %v is registered. Skipping Event", task.TaskData.TaskDetails.ID, ev.Id)
								hasevent = true
								continue
							}
						}
						if !hasevent {
							newEvents.Events = append(newEvents.Events, ev)
							log.Printf("Task %s Event %v is not registered. Including Event in Workflow Task events updates", task.TaskData.TaskDetails.ID, ev.Id)
						}
					}
				}
			}
		}
		if len(newEvents.Events) > 0 {
			log.Printf("Updating Workflow with %v new events", len(newEvents.Events))
			sort.Sort(eventsList(i.XDWEvents.Events))
			i.XDWEvents.Events = newEvents.Events
			i.XDWEvents.Count = len(newEvents.Events)
			if err := i.UpdateXDWDocumentTasks(); err != nil {
				log.Println(err.Error())
			}
		}
	}
	return nil
}
func (i *Transaction) UpdateXDWDocumentTasks() error {
	log.Printf("Updating %s Workflow Tasks with %v Events", i.XDWDocument.WorkflowDefinitionReference, len(i.XDWEvents.Events))
	for _, ev := range i.XDWEvents.Events {
		for k, wfdoctask := range i.XDWDocument.TaskList.XDWTask {
			log.Println("Checking Workflow Document Task " + wfdoctask.TaskData.TaskDetails.Name + " for matching Events")
			for inp, input := range wfdoctask.TaskData.Input {
				if ev.Expression == input.Part.Name {
					log.Println("Matched workflow document task " + wfdoctask.TaskData.TaskDetails.ID + " Input Part : " + input.Part.Name + " with Event Expression : " + ev.Expression + " Status : " + wfdoctask.TaskData.TaskDetails.Status)
					if !i.isInputRegistered(ev) {
						log.Printf("Updating XDW with Event ID %v for Task ID %s", ev.Id, wfdoctask.TaskData.TaskDetails.ID)
						i.XDWDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.AttachedTime = ev.Creationtime
						i.XDWDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.AttachedBy = ev.User + " " + ev.Org + " " + ev.Role
						i.XDWDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.HomeCommunityId = Regional_OID
						i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.LastModifiedTime = ev.Creationtime
						i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.Status = tukcnst.IN_PROGRESS
						i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.ActualOwner = ev.User + " " + ev.Org + " " + ev.Role
						if i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.ActivationTime == "" {
							i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.ActivationTime = ev.Creationtime
							log.Printf("Set Task %s Activation Time %s", wfdoctask.TaskData.TaskDetails.ID, i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.ActivationTime)
						}
						if wfdoctask.TaskData.Input[inp].Part.AttachmentInfo.AccessType == tukcnst.XDS_REGISTERED {
							i.XDWDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.Identifier = ev.XdsDocEntryUid
						} else {
							i.XDWDocument.TaskList.XDWTask[k].TaskData.Input[inp].Part.AttachmentInfo.Identifier = tukutil.GetStringFromInt(int(ev.Id))
						}
						i.newTaskEvent(ev)
						wfseqnum, _ := strconv.ParseInt(i.XDWDocument.WorkflowDocumentSequenceNumber, 0, 0)
						wfseqnum = wfseqnum + 1
						i.XDWDocument.WorkflowDocumentSequenceNumber = strconv.Itoa(int(wfseqnum))
						i.newDocEvent(ev)
					}
				}
			}
			for oup, output := range i.XDWDocument.TaskList.XDWTask[k].TaskData.Output {
				if ev.Expression == output.Part.Name {
					log.Println("Matched workflow document task " + wfdoctask.TaskData.TaskDetails.ID + " Output Part : " + output.Part.Name + " with Event Expression : " + ev.Expression + " Status : " + wfdoctask.TaskData.TaskDetails.Status)
					if !i.isOutputRegistered(ev) {
						i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.LastModifiedTime = ev.Creationtime
						i.XDWDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.AttachedTime = ev.Creationtime
						i.XDWDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.AttachedBy = ev.User + " " + ev.Org + " " + ev.Role
						i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.ActualOwner = ev.User + " " + ev.Org + " " + ev.Role
						i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.Status = tukcnst.IN_PROGRESS
						if i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.ActivationTime == "" {
							i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.ActivationTime = ev.Creationtime
						}
						if strings.HasSuffix(wfdoctask.TaskData.Output[oup].Part.AttachmentInfo.AccessType, tukcnst.XDS_REGISTERED) {
							i.XDWDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.Identifier = ev.XdsDocEntryUid
						} else {
							i.XDWDocument.TaskList.XDWTask[k].TaskData.Output[oup].Part.AttachmentInfo.Identifier = tukutil.GetStringFromInt(int(ev.Id))
						}
						i.newTaskEvent(ev)
						wfseqnum, _ := strconv.ParseInt(i.XDWDocument.WorkflowDocumentSequenceNumber, 0, 0)
						wfseqnum = wfseqnum + 1
						i.XDWDocument.WorkflowDocumentSequenceNumber = strconv.Itoa(int(wfseqnum))
						i.newDocEvent(ev)
					}
				}
			}
		}
	}

	for task := range i.XDWDocument.TaskList.XDWTask {
		if i.IsTaskCompleteBehaviorMet() {
			i.XDWDocument.TaskList.XDWTask[task].TaskData.TaskDetails.Status = tukcnst.COMPLETE
		}
	}

	if i.IsWorkflowCompleteBehaviorMet() {
		i.XDWDocument.WorkflowStatus = tukcnst.CLOSED
		tevidstr := strconv.Itoa(int(i.newEventID()))
		docevent := DocumentEvent{}
		docevent.Author = i.User
		docevent.TaskEventIdentifier = tevidstr
		docevent.EventTime = tukutil.Time_Now()
		docevent.EventType = tukcnst.COMPLETE
		docevent.PreviousStatus = i.XDWDocument.WorkflowStatusHistory.DocumentEvent[len(i.XDWDocument.WorkflowStatusHistory.DocumentEvent)-1].ActualStatus
		docevent.ActualStatus = tukcnst.COMPLETE
		i.XDWDocument.WorkflowStatusHistory.DocumentEvent = append(i.XDWDocument.WorkflowStatusHistory.DocumentEvent, docevent)
		for k := range i.XDWDocument.TaskList.XDWTask {
			i.XDWDocument.TaskList.XDWTask[k].TaskData.TaskDetails.Status = tukcnst.COMPLETE
		}
		log.Println("Closed Workflow. Total Workflow Document Events " + strconv.Itoa(len(i.XDWDocument.WorkflowStatusHistory.DocumentEvent)))
	}
	return i.updateWorkflow()
}
func (i *Transaction) isInputRegistered(ev tukdbint.Event) bool {
	log.Printf("Checking if Input Event for Task %s is registered", i.XDWDocument.TaskList.XDWTask[ev.TaskId-1].TaskData.Description)
	for _, input := range i.XDWDocument.TaskList.XDWTask[ev.TaskId-1].TaskData.Input {
		if ev.Expression == input.Part.Name {
			if input.Part.AttachmentInfo.AccessType == tukcnst.XDS_REGISTERED {
				if input.Part.AttachmentInfo.Identifier == ev.XdsDocEntryUid {
					log.Println("Event is registered. Skipping Event ")
					return true
				}
			} else {
				if input.Part.AttachmentInfo.Identifier == tukutil.GetStringFromInt(int(ev.Id)) {
					log.Println("Event is registered. Skipping Event ")
					return true
				}
			}
		}
	}
	return false
}
func (i *Transaction) isOutputRegistered(ev tukdbint.Event) bool {
	log.Printf("Checking if Ouput Event for Task %s is registered", i.XDWDocument.TaskList.XDWTask[ev.TaskId-1].TaskData.Description)
	for _, output := range i.XDWDocument.TaskList.XDWTask[ev.TaskId-1].TaskData.Output {
		if ev.Expression == output.Part.Name {
			if output.Part.AttachmentInfo.AccessType == tukcnst.XDS_REGISTERED {
				if output.Part.AttachmentInfo.Identifier == ev.XdsDocEntryUid {
					log.Println("Event is registered. Skipping Event ")
					return true
				}
			} else {
				if output.Part.AttachmentInfo.Identifier == tukutil.GetStringFromInt(int(ev.Id)) {
					log.Println("Event is registered. Skipping Event ")
					return true
				}
			}
		}
	}
	return false
}
func (i *Transaction) newTaskEvent(ev tukdbint.Event) {
	nte := TaskEvent{
		ID:         tukutil.GetStringFromInt(int(ev.Id)),
		Identifier: tukutil.GetStringFromInt(ev.TaskId),
		EventType:  i.XDWDocument.TaskList.XDWTask[ev.TaskId-1].TaskData.TaskDetails.TaskType,
		Status:     tukcnst.COMPLETE,
	}
	i.XDWDocument.TaskList.XDWTask[ev.TaskId-1].TaskEventHistory.TaskEvent = append(i.XDWDocument.TaskList.XDWTask[ev.TaskId-1].TaskEventHistory.TaskEvent, nte)
}
func (i *Transaction) newDocEvent(ev tukdbint.Event) {
	docevent := DocumentEvent{}
	docevent.Author = ev.User + " " + ev.Org + " " + ev.Role
	docevent.TaskEventIdentifier = tukutil.GetStringFromInt(ev.TaskId)
	docevent.EventTime = ev.Creationtime
	docevent.EventType = i.XDWDocument.TaskList.XDWTask[ev.TaskId-1].TaskData.TaskDetails.TaskType
	docevent.PreviousStatus = i.XDWDocument.WorkflowStatusHistory.DocumentEvent[len(i.XDWDocument.WorkflowStatusHistory.DocumentEvent)-1].ActualStatus
	docevent.ActualStatus = tukcnst.IN_PROGRESS
	i.XDWDocument.WorkflowStatusHistory.DocumentEvent = append(i.XDWDocument.WorkflowStatusHistory.DocumentEvent, docevent)
}
func (i *Transaction) updateWorkflow() error {
	var err error
	wfs := tukdbint.Workflows{Action: tukcnst.UPDATE}
	wf := tukdbint.Workflow{
		Pathway: i.Pathway,
		NHSId:   i.NHS_ID,
		XDW_Key: strings.ToUpper(i.Pathway) + i.NHS_ID,
		XDW_UID: i.XDWDocument.ID.Extension,
		Version: i.XDWVersion,
		Status:  i.XDWDocument.WorkflowStatus,
	}
	xdwDocBytes, _ := xml.MarshalIndent(i.XDWDocument, "", "  ")
	wf.XDW_Doc = string(xdwDocBytes)
	wfs.Workflows = append(wfs.Workflows, wf)
	if err = tukdbint.NewDBEvent(&wfs); err != nil {
		log.Println(err.Error())
	} else {
		log.Printf("Persisted Workflow Version %v for Pathway %s NHS ID %s", i.XDWVersion, i.Pathway, i.NHS_ID)
	}
	return err
}
func (i *Transaction) SetXDWStates() error {
	log.Println("Setting XDW States")
	var err error
	for _, wf := range i.Workflows.Workflows {
		if len(wf.XDW_Doc) > 0 {
			if err = xml.Unmarshal([]byte(wf.XDW_Doc), &i.XDWDocument); err != nil {
				log.Println(err.Error())
				return err
			}
			if err = json.Unmarshal([]byte(wf.XDW_Def), &i.XDWDefinition); err != nil {
				log.Println(err.Error())
				return err
			}
			log.Printf("Setting %s Workflow state for Patient %s", i.XDWDocument.WorkflowDefinitionReference, i.XDWDocument.Patient.ID.Extension)
			state := tukdbint.Workflowstate{}
			state.Created = wf.Created
			state.Status = wf.Status
			state.Published = wf.Published
			state.WorkflowId = wf.Id
			state.Pathway = wf.Pathway
			state.NHSId = wf.NHSId
			state.Version = wf.Version
			state.CreatedBy = i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Family + " " + i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Prefix
			state.CompleteBy = "Non Specified"
			state.LastUpdate = i.GetLatestWorkflowEventTime().String()
			state.Owner = ""
			state.Overdue = "FALSE"
			state.Escalated = "FALSE"
			state.TargetMet = "TRUE"
			state.InProgress = "TRUE"
			state.Duration = i.GetWorkflowDuration()
			state.TimeRemaining = i.GetWorkflowTimeRemaining()

			workflowStartTime := tukutil.GetTimeFromString(state.Created)
			if i.IsWorkflowOverdue() {
				i.Dashboard.TargetMissed = i.Dashboard.TargetMissed + 1
				state.Overdue = "TRUE"
				state.TargetMet = "FALSE"
			} else {
				if i.XDWDocument.WorkflowStatus == tukcnst.CLOSED {
					i.Dashboard.TargetMet = i.Dashboard.TargetMet + 1
				}
			}
			if i.XDWDefinition.CompleteByTime == "" {
				state.CompleteBy = "Non Specified"
			} else {
				period := strings.Split(i.XDWDefinition.CompleteByTime, "(")[0]
				periodDuration := tukutil.GetIntFromString(strings.Split(strings.Split(i.XDWDefinition.CompleteByTime, "(")[1], ")")[0])
				switch period {
				case "month":
					state.CompleteBy = strings.Split(tukutil.GetFutureDate(workflowStartTime, 0, periodDuration, 0, 0, 0).String(), " +")[0]
				case "day":
					state.CompleteBy = strings.Split(tukutil.GetFutureDate(workflowStartTime, 0, 0, periodDuration, 0, 0).String(), " +")[0]
				case "hour":
					state.CompleteBy = strings.Split(tukutil.GetFutureDate(workflowStartTime, 0, 0, 0, periodDuration, 0).String(), " +")[0]
				case "min":
					state.CompleteBy = strings.Split(tukutil.GetFutureDate(workflowStartTime, 0, 0, 0, 0, periodDuration).String(), " +")[0]
				}
			}

			if i.XDWDocument.WorkflowStatus == tukcnst.OPEN {
				log.Printf("Workflow %s is OPEN", wf.XDW_Key)
				i.Dashboard.InProgress = i.Dashboard.InProgress + 1
				if i.IsWorkflowEscalated() {
					log.Printf("Workflow %s is ESCALATED", wf.XDW_Key)
					i.Dashboard.Escalated = i.Dashboard.Escalated + 1
					state.Escalated = "TRUE"
				}
			} else {
				log.Printf("Workflow %s is CLOSED", wf.XDW_Key)
				i.Dashboard.Complete = i.Dashboard.Complete + 1
				state.InProgress = "FALSE"
			}
			i.XDWState.Workflowstate = append(i.XDWState.Workflowstate, state)
		}
	}

	return err
}

// IHE XDW Content Creator
func (i *Transaction) ContentCreator() error {
	log.Printf("Creating New Workflow for Pathway %s NHS ID %s", i.Pathway, i.NHS_ID)
	var err error
	if err = i.loadWorkflowDefinition(); err == nil {
		if err = i.deprecateWorkflow(); err == nil {
			i.createWorkflow()
			if err = i.persistWorkflow(); err == nil {
				err = i.persistWorkflowState()
			}
		}
	}
	if err != nil {
		log.Println(err.Error())
	}
	return err
}
func (i *Transaction) loadWorkflowDefinition() error {
	var err error
	xdwsdef := tukdbint.XDWS{Action: tukcnst.SELECT}
	xdwdef := tukdbint.XDW{Name: i.Pathway}
	xdwsdef.XDW = append(xdwsdef.XDW, xdwdef)
	if err = tukdbint.NewDBEvent(&xdwsdef); err == nil && xdwsdef.Count == 1 {
		if err = json.Unmarshal([]byte(xdwsdef.XDW[1].XDW), &i.XDWDefinition); err == nil {
			log.Printf("Loaded XDW definition for Pathway %s", i.Pathway)
		}
	} else {
		err = errors.New("no xdw definition config found")
		log.Println(err.Error())
	}
	return err
}
func (i *Transaction) deprecateWorkflow() error {
	log.Printf("Deprecating any current %s Workflow for NHS ID %s", i.Pathway, i.NHS_ID)
	var err error
	wfs := tukdbint.Workflows{Action: tukcnst.DEPRECATE}
	wf := tukdbint.Workflow{XDW_Key: i.Pathway + i.NHS_ID}
	wfs.Workflows = append(wfs.Workflows, wf)
	if err = tukdbint.NewDBEvent(&wfs); err == nil {
		log.Printf("Deprecating any current %s Workflow events for NHS ID %s", i.Pathway, i.NHS_ID)
		evs := tukdbint.Events{Action: tukcnst.DEPRECATE}
		ev := tukdbint.Event{Pathway: i.Pathway, NhsId: i.NHS_ID}
		evs.Events = append(evs.Events, ev)
		if err = tukdbint.NewDBEvent(&evs); err != nil {
			log.Println(err.Error())
		}
	}
	return err
}
func (i *Transaction) createWorkflow() {
	i.Expression = "Create Task"
	var authoid = getLocalId(i.Org)
	var patoid = tukcnst.NHS_OID_DEFAULT
	var wfid = tukutil.Newid()
	var effectiveTime = tukutil.Time_Now()
	i.XDWDocument.Xdw = tukcnst.XDWNameSpace
	i.XDWDocument.Hl7 = tukcnst.HL7NameSpace
	i.XDWDocument.WsHt = tukcnst.WHTNameSpace
	i.XDWDocument.Xsi = tukcnst.XMLNS_XSI
	i.XDWDocument.XMLName.Local = tukcnst.XDWNameLocal
	i.XDWDocument.SchemaLocation = tukcnst.WorkflowDocumentSchemaLocation
	i.XDWDocument.ID.Root = strings.ReplaceAll(tukcnst.WorkflowInstanceId, "^", "")
	i.XDWDocument.ID.Extension = wfid
	i.XDWDocument.ID.AssigningAuthorityName = strings.ToUpper(i.Org)
	i.XDWDocument.EffectiveTime.Value = effectiveTime
	i.XDWDocument.ConfidentialityCode.Code = i.XDWDefinition.Confidentialitycode
	i.XDWDocument.Patient.ID.Root = patoid
	i.XDWDocument.Patient.ID.Extension = i.NHS_ID
	i.XDWDocument.Patient.ID.AssigningAuthorityName = "NHS"
	i.XDWDocument.Author.AssignedAuthor.ID.Root = authoid
	i.XDWDocument.Author.AssignedAuthor.ID.Extension = strings.ToUpper(i.Org)
	i.XDWDocument.Author.AssignedAuthor.ID.AssigningAuthorityName = authoid
	i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Family = i.User
	i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Prefix = i.Role
	i.XDWDocument.WorkflowInstanceId = wfid + tukcnst.WorkflowInstanceId
	i.XDWDocument.WorkflowDocumentSequenceNumber = "1"
	i.XDWDocument.WorkflowStatus = tukcnst.OPEN
	i.XDWDocument.WorkflowDefinitionReference = strings.ToUpper(i.Pathway)
	for _, t := range i.XDWDefinition.Tasks {
		i.Expression = t.Name
		i.Task_ID = tukutil.GetIntFromString(t.ID)
		tevidstr := tukutil.GetStringFromInt(int(i.newEventID()))
		log.Printf("Creating Workflow Task ID - %v Name - %s", t.ID, t.Name)
		task := XDWTask{}
		task.TaskData.TaskDetails.ID = t.ID
		task.TaskData.TaskDetails.TaskType = t.Tasktype
		task.TaskData.TaskDetails.Name = t.Name
		task.TaskData.TaskDetails.ActualOwner = t.ActualOwner
		task.TaskData.TaskDetails.CreatedBy = i.Role + " " + i.User
		task.TaskData.TaskDetails.CreatedTime = effectiveTime
		task.TaskData.TaskDetails.RenderingMethodExists = "false"
		task.TaskData.TaskDetails.LastModifiedTime = effectiveTime
		task.TaskData.Description = t.Description
		task.TaskData.TaskDetails.Status = tukcnst.CREATED
		for _, inp := range t.Input {
			docinput := Input{}
			docinput.Part.Name = inp.Name
			docinput.Part.AttachmentInfo.Name = inp.Name
			docinput.Part.AttachmentInfo.AccessType = inp.AccessType
			docinput.Part.AttachmentInfo.ContentType = inp.Contenttype
			docinput.Part.AttachmentInfo.ContentCategory = tukcnst.MEDIA_TYPES
			task.TaskData.Input = append(task.TaskData.Input, docinput)
			log.Printf("Created Input Part - %s", inp.Name)
		}
		for _, outp := range t.Output {
			docoutput := Output{}
			docoutput.Part.Name = outp.Name
			docoutput.Part.AttachmentInfo.Name = outp.Name
			docoutput.Part.AttachmentInfo.AccessType = outp.AccessType
			docoutput.Part.AttachmentInfo.ContentType = outp.Contenttype
			docoutput.Part.AttachmentInfo.ContentCategory = tukcnst.MEDIA_TYPES
			task.TaskData.Output = append(task.TaskData.Output, docoutput)
			log.Printf("Created Output Part - %s", outp.Name)
		}
		tev := TaskEvent{}
		tev.EventTime = effectiveTime
		tev.ID = tevidstr
		tev.Identifier = t.ID
		tev.EventType = tukcnst.XDW_TASKEVENTTYPE_CREATED
		tev.Status = tukcnst.XDW_TASKEVENTTYPE_COMPLETE
		task.TaskEventHistory.TaskEvent = append(task.TaskEventHistory.TaskEvent, tev)
		i.XDWDocument.TaskList.XDWTask = append(i.XDWDocument.TaskList.XDWTask, task)
		log.Printf("Set Workflow Task Event %s %s status to %s", t.ID, tev.EventType, tev.Status)
	}
	docevent := DocumentEvent{}
	docevent.Author = i.User + " " + i.Role
	docevent.TaskEventIdentifier = "1"
	docevent.EventTime = effectiveTime
	docevent.EventType = tukcnst.CREATED
	docevent.ActualStatus = tukcnst.OPEN
	i.XDWDocument.WorkflowStatusHistory.DocumentEvent = append(i.XDWDocument.WorkflowStatusHistory.DocumentEvent, docevent)
	i.Response, _ = xml.MarshalIndent(i.XDWDocument, "", "  ")
	i.XDWVersion = 0
	log.Printf("%s Created new %s Workflow for Patient %s", i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Family, i.XDWDocument.WorkflowDefinitionReference, i.NHS_ID)
}
func (i *Transaction) persistWorkflow() error {
	var err error
	wfs := tukdbint.Workflows{Action: tukcnst.INSERT}
	wf := tukdbint.Workflow{
		Pathway: i.Pathway,
		NHSId:   i.NHS_ID,
		XDW_Key: strings.ToUpper(i.Pathway) + i.NHS_ID,
		XDW_UID: i.XDWDocument.ID.Extension,
		Version: i.XDWVersion,
		Status:  i.XDWDocument.WorkflowStatus,
	}
	xdwDocBytes, _ := xml.MarshalIndent(i.XDWDocument, "", "  ")
	xdwDefBytes, _ := json.Marshal(i.XDWDefinition)
	wf.XDW_Doc = string(xdwDocBytes)
	wf.XDW_Def = string(xdwDefBytes)
	wfs.Workflows = append(wfs.Workflows, wf)
	if err = tukdbint.NewDBEvent(&wfs); err != nil {
		log.Println(err.Error())
	} else {
		log.Printf("Persisted Workflow Version %v for Pathway %s NHS ID %s", i.XDWVersion, i.Pathway, i.NHS_ID)
	}
	i.Workflows.LastInsertId = wfs.LastInsertId
	return err
}
func (i *Transaction) persistWorkflowState() error {
	states := tukdbint.WorkflowStates{Action: tukcnst.INSERT}
	state := tukdbint.Workflowstate{
		WorkflowId:    i.Workflows.LastInsertId,
		Pathway:       i.Pathway,
		NHSId:         i.NHS_ID,
		Version:       i.XDWVersion,
		Published:     false,
		Created:       i.XDWDocument.EffectiveTime.Value,
		CreatedBy:     i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Family + " " + i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Prefix,
		Status:        i.XDWDocument.WorkflowStatus,
		CompleteBy:    i.GetWorkflowCompleteByDate().String(),
		LastUpdate:    i.XDWDocument.EffectiveTime.Value,
		Owner:         "",
		Overdue:       "FALSE",
		Escalated:     "FALSE",
		TargetMet:     "TRUE",
		InProgress:    "FALSE",
		Duration:      "0 mins",
		TimeRemaining: i.GetWorkflowTimeRemaining(),
	}
	states.Workflowstate = append(states.Workflowstate, state)
	return tukdbint.NewDBEvent(&states)
}

// IHE XDW Content Consumer
func (i *Transaction) ContentConsumer() error {
	i.Workflows = tukdbint.Workflows{Action: tukcnst.SELECT}
	wf := tukdbint.Workflow{Pathway: i.Pathway, NHSId: i.NHS_ID, Version: i.XDWVersion}
	i.Workflows.Workflows = append(i.Workflows.Workflows, wf)
	if err := tukdbint.NewDBEvent(&i.Workflows); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("Selected %v Workflows", i.Workflows.Count)
	i.Dashboard.Total = i.Workflows.Count
	if err := i.SetXDWStates(); err != nil {
		log.Println(err.Error())
		return err
	}
	return nil
}

func GetActiveWorkflowNames() []string {
	var names []string
	wfnames := tukdbint.GetActiveWorkflowNames()
	for name := range wfnames {
		names = append(names, name)
	}
	return names
}
func GetTaskNotes(pwy string, nhsid string, taskid int, ver int) string {
	return tukdbint.GetTaskNotes(pwy, nhsid, taskid, ver)
}
func (i *Transaction) IsTaskOverdue() bool {
	log.Printf("Checking if Workflow %s Task %v is overdue", i.Pathway, i.Task_ID)
	completionDate := i.GetTaskCompleteByDate()
	log.Printf("Task complete by time %s", completionDate)
	if time.Now().Local().Before(completionDate) {
		log.Printf("Time Now is before Task Complete by date. Task %v is NOT overdue", i.Task_ID)
		return false
	}
	if i.XDWDocument.TaskList.XDWTask[i.Task_ID-1].TaskData.TaskDetails.Status == tukcnst.COMPLETE {
		log.Printf("Task %v is Complete. Checking latest task event time", i.Task_ID)
		lasteventime := tukutil.GetTimeFromString(i.XDWDocument.TaskList.XDWTask[i.Task_ID-1].TaskData.TaskDetails.LastModifiedTime)
		if lasteventime.Before(completionDate) {
			log.Printf("Task %v was NOT overdue", i.Task_ID)
			return false
		}
	}
	log.Printf("Task %v IS overdue", i.Task_ID)
	return true
}
func GetTaskCompleteByDate(xdwdoc XDWWorkflowDocument, xdwdef WorkflowDefinition, task int) string {
	trans := Transaction{XDWDocument: xdwdoc, XDWDefinition: xdwdef, Task_ID: task}
	return strings.Split(trans.GetTaskCompleteByDate().String(), ".")[0]
}
func (i *Transaction) GetTaskCompleteByDate() time.Time {
	if i.XDWDefinition.Tasks[i.Task_ID-1].CompleteByTime == "" {
		return i.GetWorkflowCompleteByDate()
	}
	return tukutil.OHT_FutureDate(tukutil.GetTimeFromString(i.XDWDocument.EffectiveTime.Value), i.XDWDefinition.Tasks[i.Task_ID-1].CompleteByTime)
}
func (i *Transaction) GetWorkflowDuration() string {
	ws := tukutil.GetTimeFromString(i.XDWDocument.EffectiveTime.Value)
	log.Printf("Workflow Started %s Status %s", ws.String(), i.XDWDocument.WorkflowStatus)
	we := time.Now()
	log.Printf("Time Now %s", we.String())
	if i.XDWDocument.WorkflowStatus == tukcnst.CLOSED {
		we = i.GetLatestWorkflowEventTime()
		log.Printf("Workflow is Complete. Latest Event Time was %s", we.String())
	}
	duration := we.Sub(ws)
	log.Println("Duration - " + duration.String())
	return tukutil.GetDuration(ws.String(), we.String())
}
func (i *Transaction) GetLatestWorkflowEventTime() time.Time {
	var ltime = tukutil.GetTimeFromString(i.XDWDocument.EffectiveTime.Value)
	for _, task := range i.XDWDocument.TaskList.XDWTask {
		for _, taskevent := range task.TaskEventHistory.TaskEvent {
			if taskevent.EventTime != "" {
				etime := tukutil.GetTimeFromString(taskevent.EventTime)
				if etime.After(ltime) {
					ltime = etime
				}
			}
		}
	}
	return ltime
}
func (i *Transaction) newEventID() int64 {
	ev := tukdbint.Event{
		DocName:            i.XDWDocument.WorkflowDefinitionReference + "-" + i.NHS_ID,
		ClassCode:          i.XDSDocumentMeta.Classcode,
		ConfCode:           i.XDSDocumentMeta.Confcode,
		FormatCode:         i.XDSDocumentMeta.Formatcode,
		FacilityCode:       i.XDSDocumentMeta.Facilitycode,
		PracticeCode:       i.XDSDocumentMeta.Practicesettingcode,
		Expression:         i.Expression,
		Authors:            i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Prefix + " " + i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Family,
		XdsPid:             "",
		XdsDocEntryUid:     i.XDWDocument.ID.Extension,
		RepositoryUniqueId: i.XDSDocumentMeta.Repositoryuniqueid,
		NhsId:              i.NHS_ID,
		User:               i.User,
		Org:                i.Org,
		Role:               i.Role,
		Topic:              tukcnst.DSUB_TOPIC_TYPE_CODE,
		Pathway:            i.Pathway,
		Comments:           string(i.Request),
		Version:            0,
		TaskId:             i.Task_ID,
	}
	evs := tukdbint.Events{Action: tukcnst.INSERT}
	evs.Events = append(evs.Events, ev)
	if err := tukdbint.NewDBEvent(&evs); err != nil {
		log.Println(err.Error())
		return 0
	}
	log.Printf("Created Event ID :  = %v", evs.LastInsertId)
	return evs.LastInsertId
}
func (i *Transaction) IsWorkflowOverdue() bool {
	if i.XDWDefinition.CompleteByTime != "" {
		completebyDate := i.GetWorkflowCompleteByDate()
		log.Printf("Workflow Complete By Date %s", completebyDate.String())
		if time.Now().After(completebyDate) {
			log.Printf("Time Now is after Workflow Complete By Date %s", completebyDate.String())
			if i.XDWDocument.WorkflowStatus == tukcnst.CLOSED {
				log.Println("Workflow is Complete, Obtaining latest workflow event time")
				levent := i.GetLatestWorkflowEventTime()
				log.Printf("Workflow Latest Event Time %s. Workflow Target Met = %v", levent.String(), levent.Before(completebyDate))
				return levent.After(completebyDate)
			} else {
				log.Printf("Workflow is not Complete. Complete By Date is %s Workflow Target not met", completebyDate.String())
				return true
			}
		} else {
			log.Printf("Time Now is before Workflow Complete By Date %s. Workflow is not overdue", completebyDate.String())
			return false
		}
	}
	log.Println("Workflow definition does not specify a Complete By Time. Workflow is not overdue")
	return false
}
func (i *Transaction) GetWorkflowCompleteByDate() time.Time {
	return tukutil.OHT_FutureDate(tukutil.GetTimeFromString(i.XDWDocument.EffectiveTime.Value), i.XDWDefinition.CompleteByTime)
}
func (i *Transaction) IsWorkflowCompleteBehaviorMet() bool {
	var conditions []string
	var completedConditions = 0
	for _, cc := range i.XDWDefinition.CompletionBehavior {
		if cc.Completion.Condition != "" {
			log.Println("Parsing Workflow Completion Condition " + cc.Completion.Condition)
			if strings.Contains(cc.Completion.Condition, " and ") {
				conditions = strings.Split(cc.Completion.Condition, " and ")
			} else {
				conditions = append(conditions, cc.Completion.Condition)
			}
			for _, condition := range conditions {
				endMethodInd := strings.Index(condition, "(")
				if endMethodInd > 0 {
					method := cc.Completion.Condition[0:endMethodInd]
					if method != "task" {
						log.Println(method + " is an Invalid Workflow Completion Behaviour Condition method. Ignoring Condition")
						continue
					}
					endParamInd := strings.Index(cc.Completion.Condition, ")")
					param := cc.Completion.Condition[endMethodInd+1 : endParamInd]
					for _, task := range i.XDWDocument.TaskList.XDWTask {
						if task.TaskData.TaskDetails.ID == param {
							if task.TaskData.TaskDetails.Status == tukcnst.COMPLETE {
								completedConditions = completedConditions + 1
							}
						}
					}
				}
			}
		}
	}
	if len(conditions) == completedConditions {
		log.Printf("%s Workflow for NHS ID %s is complete", i.Pathway, i.NHS_ID)
		return true
	}
	log.Printf("%s Workflow for NHS ID %s is not complete", i.Pathway, i.NHS_ID)
	return false
}
func IsLatestTaskEvent(i XDWWorkflowDocument, task int, taskEventName string) bool {
	var lastoutputtime = tukutil.GetTimeFromString(i.EffectiveTime.Value)
	var lastinputtime = lastoutputtime
	var latestInputTaskEvent string
	var latestOutputTaskEvent string
	for _, in := range i.TaskList.XDWTask[task].TaskData.Input {
		if in.Part.AttachmentInfo.AttachedTime != "" {
			inputtime := tukutil.GetTimeFromString(in.Part.AttachmentInfo.AttachedTime)
			if inputtime.After(lastinputtime) {
				lastinputtime = inputtime
				latestInputTaskEvent = in.Part.AttachmentInfo.Name
			}
		}
	}
	for _, op := range i.TaskList.XDWTask[task].TaskData.Output {
		if op.Part.AttachmentInfo.AttachedTime != "" {
			outputtime := tukutil.GetTimeFromString(op.Part.AttachmentInfo.AttachedTime)
			if outputtime.After(lastoutputtime) {
				lastoutputtime = outputtime
				latestOutputTaskEvent = op.Part.AttachmentInfo.Name
			}
		}
	}
	if lastoutputtime.After(lastinputtime) {
		if taskEventName == latestOutputTaskEvent {
			return true
		}
	} else {
		if taskEventName == latestInputTaskEvent {
			return true
		}
	}
	return false
}
func (i *Transaction) IsTaskCompleteBehaviorMet() bool {
	log.Printf("Checking if Task %v is complete", i.Task_ID)
	var conditions []string
	var completedConditions = 0
	for _, cond := range i.XDWDefinition.Tasks[i.Task_ID].CompletionBehavior {
		log.Printf("Task %v Completion Condition is %s", i.Task_ID, cond)
		if cond.Completion.Condition != "" {
			if strings.Contains(cond.Completion.Condition, " and ") {
				conditions = strings.Split(cond.Completion.Condition, " and ")
			} else {
				conditions = append(conditions, cond.Completion.Condition)
			}
			log.Printf("Checkign Task %v %v completion conditions", i.Task_ID, len(conditions))
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
					log.Printf("Completion condition is %s", method)
					switch method {
					case "output":
						for _, op := range i.XDWDocument.TaskList.XDWTask[i.Task_ID].TaskData.Output {
							if op.Part.AttachmentInfo.AttachedTime != "" && op.Part.AttachmentInfo.Name == param {
								completedConditions = completedConditions + 1
							}
						}
					case "input":
						for _, in := range i.XDWDocument.TaskList.XDWTask[i.Task_ID].TaskData.Input {
							if in.Part.AttachmentInfo.AttachedTime != "" && in.Part.AttachmentInfo.Name == param {
								completedConditions = completedConditions + 1
							}
						}
					case "task":
						for _, task := range i.XDWDocument.TaskList.XDWTask {
							if task.TaskData.TaskDetails.ID == param {
								if task.TaskData.TaskDetails.Status == tukcnst.COMPLETE {
									completedConditions = completedConditions + 1
								}
							}
						}
					case "latest":
						if i.getLatestTaskEvent() == param {
							completedConditions = completedConditions + 1
						}
					}
				}
			}
		}
	}
	if len(conditions) == completedConditions {
		log.Printf("Task %v is complete", i.Task_ID)
		return true
	}
	log.Printf("Task %v is not complete", i.Task_ID)
	return false
}
func (i *Transaction) getLatestTaskEvent() string {
	var lasteventtime = tukutil.GetTimeFromString(i.XDWDocument.EffectiveTime.Value)
	var lastevent = ""
	for _, v := range i.XDWDocument.TaskList.XDWTask[i.Task_ID].TaskData.Input {
		if v.Part.AttachmentInfo.AttachedTime != "" {
			et := tukutil.GetTimeFromString(v.Part.AttachmentInfo.AttachedTime)
			if et.After(lasteventtime) {
				lasteventtime = et
				lastevent = v.Part.AttachmentInfo.Name
			}
		}
	}
	for _, v := range i.XDWDocument.TaskList.XDWTask[i.Task_ID].TaskData.Output {
		if v.Part.AttachmentInfo.AttachedTime != "" {
			et := tukutil.GetTimeFromString(v.Part.AttachmentInfo.AttachedTime)
			if et.After(lasteventtime) {
				lasteventtime = et
				lastevent = v.Part.AttachmentInfo.Name
			}
		}
	}
	return lastevent
}
func (i *Transaction) GetTaskDuration() string {
	taskCreationTime := tukutil.GetTimeFromString(i.XDWDocument.EffectiveTime.Value)
	log.Printf("Task %v Creation Time %s", i.Task_ID, taskCreationTime.String())
	if i.XDWDocument.TaskList.XDWTask[i.Task_ID-1].TaskData.TaskDetails.Status == tukcnst.COMPLETE {
		log.Printf("Workflow Task %s is complete", i.XDWDocument.TaskList.XDWTask[i.Task_ID-1].TaskData.TaskDetails.Name)
		lastEvent := tukutil.GetTimeFromString(i.XDWDocument.TaskList.XDWTask[i.Task_ID-1].TaskData.TaskDetails.LastModifiedTime)
		log.Printf("Lastest Task Event %s", lastEvent.String())
		duration := lastEvent.Sub(taskCreationTime)
		log.Printf("Task %v %s Created %s Status is COMPLETE Duration - %s", i.Task_ID, i.XDWDocument.TaskList.XDWTask[i.Task_ID-1].TaskData.Description, taskCreationTime.String(), duration.String())
		return tukutil.PrettyPrintDuration(duration)
	} else {
		duration := time.Since(taskCreationTime)
		log.Printf("Task %v %s Created %s Status is %s Duration - %s", i.Task_ID, i.XDWDocument.TaskList.XDWTask[i.Task_ID-1].TaskData.Description, taskCreationTime.String(), i.XDWDocument.TaskList.XDWTask[i.Task_ID-1].TaskData.TaskDetails.Status, duration.String())
		return tukutil.PrettyPrintDuration(duration)
	}
}
func (i *Transaction) GetTaskTimeRemaining() string {
	taskCreateTime := tukutil.GetTimeFromString(i.XDWDocument.EffectiveTime.Value)
	taskCompleteby := tukutil.OHT_FutureDate(taskCreateTime, i.XDWDefinition.Tasks[i.Task_ID-1].CompleteByTime)
	log.Printf("Completion time %s", taskCompleteby.String())
	if time.Now().After(taskCompleteby) {
		return "0"
	}
	timeRemaining := taskCompleteby.Sub(taskCreateTime)
	log.Println("Task Time Remaining : " + timeRemaining.String())
	return tukutil.PrettyPrintDuration(timeRemaining)
}
func (i *Transaction) GetWorkflowTimeRemaining() string {
	createTime := tukutil.GetTimeFromString(i.XDWDocument.EffectiveTime.Value)
	completeby := tukutil.OHT_FutureDate(createTime, i.XDWDefinition.CompleteByTime)
	log.Printf("Completion time %s", completeby.String())
	if time.Now().After(completeby) {
		return "0"
	}
	timeRemaining := time.Until(completeby)
	log.Println("Workflow Time Remaining : " + timeRemaining.String())
	return tukutil.PrettyPrintDuration(timeRemaining)
}
func getLocalId(mid string) string {
	return tukdbint.GetIDMapsLocalId(mid)
}
func (i *Transaction) IsWorkflowEscalated() bool {
	if i.XDWDefinition.ExpirationTime != "" {
		escalatedate := tukutil.OHT_FutureDate(tukutil.GetTimeFromString(i.XDWDocument.EffectiveTime.Value), i.XDWDefinition.ExpirationTime)
		log.Printf("Workflow Start Time %s Worklow Escalate Time %s Workflow Escaleted = %v", i.XDWDocument.EffectiveTime.Value, escalatedate.String(), time.Now().After(escalatedate))
		return time.Now().After(escalatedate)
	}
	log.Println("No Escalate time defined for Workflow")
	return false
}

// XDW Admin functions

func (i *Transaction) RegisterWorkflowDefinition(ismeta bool) error {
	var err error
	if i.Pathway == "" {
		return errors.New("pathway is not set")
	}
	if i.Request == nil || string(i.Request) == "" {
		return errors.New("request bytes is not set")
	}
	if ismeta {
		log.Println("Persisting Workflow XDS Meta")
		err = i.registerWorkflowXDSMeta()
	} else {
		log.Println("Registering Workflow Definition")
		err = i.registerWorkflowDef()
	}
	return err
}
func (i *Transaction) registerWorkflowDef() error {
	var err error
	err = json.Unmarshal(i.Request, &i.XDWDefinition)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	event := tukdsub.DSUBEvent{Action: tukcnst.CANCEL, Pathway: i.XDWDefinition.Ref}
	tukdsub.New_Transaction(&event)
	log.Printf("Cancelled any existing DSUB Broker and Event Service Subscriptions for Pathway %s", i.XDWDefinition.Ref)
	dsubSubs := make(map[string]string)
	eventSubs := make(map[string]string)
	if err = i.PersistXDWDefinition(); err == nil {
		log.Println("Parsing XDW Tasks for potential DSUB Broker Subscriptions")
		for _, task := range i.XDWDefinition.Tasks {
			for _, inp := range task.Input {
				log.Printf("Checking Task %v %s input %s", task.ID, task.Name, inp.Name)
				switch inp.AccessType {
				case tukcnst.XDS_REGISTERED:
					dsubSubs[inp.Name] = i.XDWDefinition.Ref
					log.Printf("Task %v %s input %s included in potential DSUB Broker subscriptions", task.ID, task.Name, inp.Name)
				case "URL":
					eventSubs[inp.Name] = i.XDWDefinition.Ref
					log.Printf("Task %v %s input %s included in potential User Event subscriptions", task.ID, task.Name, inp.Name)
				}
			}
			for _, out := range task.Output {
				log.Printf("Checking Task %v %s output %s", task.ID, task.Name, out.Name)
				switch out.AccessType {
				case tukcnst.XDS_REGISTERED:
					dsubSubs[out.Name] = i.XDWDefinition.Ref
					log.Printf("Task %v %s input %s included in potential DSUB Broker subscriptions", task.ID, task.Name, out.Name)
				case "URL":
					eventSubs[out.Name] = i.XDWDefinition.Ref
					log.Printf("Task %v %s input %s included in potential User Event subscriptions", task.ID, task.Name, out.Name)
				}
			}
		}
	}
	log.Printf("Found %v potential DSUB Broker Subscriptions - %s", len(dsubSubs), dsubSubs)
	if len(dsubSubs) > 0 {
		event.Action = tukcnst.CREATE
		event.BrokerURL = i.DSUB_BrokerURL
		event.ConsumerURL = i.DSUB_ConsumerURL
		for expression := range dsubSubs {
			event.Expressions = append(event.Expressions, expression)
		}
		err = tukdsub.New_Transaction(&event)
	}
	if len(eventSubs) > 0 {
		for exp, wf := range eventSubs {
			subs := tukdbint.Subscriptions{Action: tukcnst.SELECT}
			sub := tukdbint.Subscription{Pathway: wf, Topic: "URL", Expression: exp}
			subs.Subscriptions = append(subs.Subscriptions, sub)
			tukdbint.NewDBEvent(&subs)
			if subs.Count == 0 {
				subs := tukdbint.Subscriptions{Action: tukcnst.INSERT}
				sub := tukdbint.Subscription{BrokerRef: "Event_Service", Pathway: wf, Topic: "URL", Expression: exp}
				subs.Subscriptions = append(subs.Subscriptions, sub)
				tukdbint.NewDBEvent(&subs)
			}
		}
	}
	return err
}
func (i *Transaction) registerWorkflowXDSMeta() error {
	var err error
	xdw := tukdbint.XDW{Name: i.Pathway + "_meta", IsXDSMeta: true}
	xdws := tukdbint.XDWS{Action: tukcnst.DELETE}
	xdws.XDW = append(xdws.XDW, xdw)
	if err = tukdbint.NewDBEvent(&xdws); err == nil {
		log.Printf("Deleted Existing XDS Meta for Pathway %s", i.Pathway)
		xdw = tukdbint.XDW{Name: i.Pathway + "_meta", IsXDSMeta: true, XDW: string(i.Request)}
		xdws = tukdbint.XDWS{Action: tukcnst.INSERT}
		xdws.XDW = append(xdws.XDW, xdw)
		if err = tukdbint.NewDBEvent(&xdws); err == nil {
			log.Printf("Persisted Workflow XDS Meta for Pathway %s", i.Pathway)
		}
	}
	return err
}
func (i *Transaction) PersistXDWDefinition() error {
	var err error
	xdw := tukdbint.XDW{Name: i.Pathway, IsXDSMeta: false}
	xdws := tukdbint.XDWS{Action: tukcnst.DELETE}
	xdws.XDW = append(xdws.XDW, xdw)
	if err = tukdbint.NewDBEvent(&xdws); err == nil {
		xdw = tukdbint.XDW{Name: i.Pathway, IsXDSMeta: false, XDW: string(i.Request)}
		xdws = tukdbint.XDWS{Action: tukcnst.INSERT}
		xdws.XDW = append(xdws.XDW, xdw)
		if err = tukdbint.NewDBEvent(&xdws); err == nil {
			log.Printf("Persisted New XDW Definition for Pathway %s", i.Pathway)
		}
	}
	return err
}
func GetWorkflowDefinitionNames() []string {
	var wfnames []string
	names := tukdbint.GetWorkflowDefinitionNames()
	for name := range names {
		wfnames = append(wfnames, name)
	}
	return wfnames
}
func GetWorkflowXDSMetaNames() []string {
	return tukdbint.GetWorkflowXDSMetaNames()
}

// sort interface
type DocumentEvents []DocumentEvent

func (e DocumentEvents) Len() int {
	return len(e)
}
func (e DocumentEvents) Less(i, j int) bool {
	return e[i].EventTime > e[j].EventTime
}
func (e DocumentEvents) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

type eventsList []tukdbint.Event

func (e eventsList) Len() int {
	return len(e)
}
func (e eventsList) Less(i, j int) bool {
	return e[i].Id > e[j].Id
}
func (e eventsList) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
