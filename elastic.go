package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/gospider007/gson"
	"github.com/gospider007/requests"
)

type Query struct {
	query map[string]any
}

func NewQuery() *Query {
	return &Query{
		query: make(map[string]any),
	}
}
func (obj *Query) AddRange(ranges ...map[string]any) {
	ran, ok := obj.query["range"]
	if ok {
		obj.query["range"] = append(ran.([]map[string]any), ranges...)
	} else {
		obj.query["range"] = ranges
	}
}
func (obj *Query) AddFilter(filters ...map[string]any) {
	filter, ok := obj.query["filter"]
	if ok {
		obj.query["filter"] = append(filter.([]map[string]any), filters...)
	} else {
		obj.query["filter"] = filters
	}
}
func (obj *Query) AddMust(musts ...map[string]any) {
	must, ok := obj.query["must"]
	if ok {
		obj.query["must"] = append(must.([]map[string]any), musts...)
	} else {
		obj.query["must"] = musts
	}
}
func (obj *Query) AddMustNot(musts ...map[string]any) {
	must_not, ok := obj.query["must_not"]
	if ok {
		obj.query["must_not"] = append(must_not.([]map[string]any), musts...)
	} else {
		obj.query["must_not"] = musts
	}
}
func (obj *Query) AddShoud(shoulds ...map[string]any) {
	should, ok := obj.query["should"]
	if ok {
		obj.query["should"] = append(should.([]map[string]any), shoulds...)
	} else {
		obj.query["should"] = shoulds
	}
	obj.query["minimum_should_match"] = 1
}
func (obj *Query) Query() map[string]any {
	return map[string]any{
		"bool": obj.query,
	}
}

type Client struct {
	reqCli  *requests.Client
	baseUrl string
}

type ClientOption struct {
	Host  string
	Port  int
	Usr   string
	Pwd   string
	Ssl   bool
	Proxy string
}

func getBaseUrl(option ClientOption) (string, error) {
	var baseUrl string
	if option.Ssl {
		baseUrl += "https://"
	} else {
		baseUrl += "http://"
	}
	if option.Usr != "" && option.Pwd != "" {
		baseUrl += fmt.Sprintf("%s:%s@%s", option.Usr, option.Pwd, net.JoinHostPort(option.Host, strconv.Itoa(option.Port)))
	} else {
		baseUrl += net.JoinHostPort(option.Host, strconv.Itoa(option.Port))
	}
	return baseUrl, nil
}

func (obj Client) Ping(ctx context.Context) error {
	_, err := obj.reqCli.Request(ctx, "get", obj.baseUrl)
	if err != nil {
		return err
	}
	return nil
}

type UpdateData struct {
	Index string
	Id    string
	Data  any
}
type DeleteData struct {
	Index string
	Id    string
}
type SearchResult struct {
	Total int64
	Datas []*gson.Client
}
type SearchResults struct {
	scroll_id string
	datas     []*gson.Client
	init      bool
	client    *Client
	err       error
	closed    bool
}

func (obj *SearchResults) Next(ctx context.Context) (ok bool) {
	if obj.closed {
		return false
	}
	if !obj.init {
		obj.init = true
	} else {
		obj.scroll_id, obj.err = obj.scroll(ctx)
	}
	if len(obj.datas) == 0 || obj.err != nil {
		obj.closed = true
		ok = false
	} else {
		ok = true
	}
	if obj.closed {
		obj.Close()
	}
	return
}

func (obj *SearchResults) scroll(ctx context.Context) (scroll_id string, err error) {
	defer func() {
		if scroll_id != obj.scroll_id {
			obj.Close()
		}
	}()
	data := map[string]any{
		"scroll":    "5m",
		"scroll_id": obj.scroll_id,
	}
	href := obj.client.baseUrl + "/_search/scroll"
	jsonData, err := obj.client.search(ctx, "post", href, data)
	if err != nil {
		return "", err
	}
	scroll_id = jsonData.Get("_scroll_id").String()
	obj.datas = jsonData.Get("hits.hits").Array()
	log.Print(len(obj.datas))
	return scroll_id, nil
}
func (obj *SearchResults) Datas() []*gson.Client {
	if !obj.init {
		return nil
	}
	return obj.datas
}
func (obj *SearchResults) Error() error {
	return obj.err
}
func (obj *SearchResults) Close() error {
	if obj.scroll_id == "" {
		return nil
	}
	log.Print("close scroll: ", obj.scroll_id)
	data := map[string]any{
		"scroll_id": []string{
			obj.scroll_id,
		},
	}
	href := obj.client.baseUrl + "/_search/scroll"
	_, err := obj.client.search(context.TODO(), "delete", href, data)
	return err
}
func (obj *Client) Count(ctx context.Context, index string, data any) (int64, error) {
	href := obj.baseUrl + fmt.Sprintf("/%s/_count", index)
	jsonData, err := obj.search(ctx, "post", href, data)
	if err != nil {
		return 0, err
	}
	countRs := jsonData.Get("count")
	if !countRs.Exists() {
		return 0, errors.New("not found count")
	}
	return countRs.Int(), nil
}

func (obj *Client) Close() {
	obj.reqCli.ForceCloseConns()
}

func (obj *Client) Search(ctx context.Context, index string, data any) (SearchResult, error) {
	var searchResult SearchResult
	href := obj.baseUrl + fmt.Sprintf("/%s/_search", index)

	jsonData, err := obj.search(ctx, "post", href, data)
	if err != nil {
		return searchResult, err
	}
	hits := jsonData.Get("hits")
	if !hits.Exists() {
		return searchResult, errors.New("not found hits")
	}
	searchResult.Total = hits.Get("total.value").Int()
	searchResult.Datas = hits.Get("hits").Array()
	return searchResult, nil
}
func (obj *Client) Searchs(ctx context.Context, index string, data any) (*SearchResults, error) {
	href := obj.baseUrl + fmt.Sprintf("/%s/_search?scroll=5m", index)
	jsonData, err := obj.search(ctx, "post", href, data)
	if err != nil {
		return nil, err
	}
	hits := jsonData.Get("hits")
	if !hits.Exists() {
		return nil, errors.New("not found hits")
	}
	return &SearchResults{
		scroll_id: jsonData.Get("_scroll_id").String(),
		datas:     hits.Get("hits").Array(),
		client:    obj,
	}, nil
}

func (obj *Client) Exists(ctx context.Context, index, id string) (bool, error) {
	href := obj.baseUrl + fmt.Sprintf("/%s/_count?q=_id:%s", index, id)
	jsonData, err := obj.search(ctx, "get", href, nil)
	if err != nil {
		return false, err
	}
	countRs := jsonData.Get("count")
	if !countRs.Exists() {
		return false, errors.New("not found count")
	}
	if countRs.Int() > 0 {
		return true, nil
	}
	return false, nil
}
func (obj *Client) Delete(ctx context.Context, deleteDatas ...DeleteData) error {
	bulkDatas := []BulkData{}
	for _, data := range deleteDatas {
		bulkData := BulkData{
			Method: Delete,
			Index:  data.Index,
			Id:     data.Id,
		}
		bulkDatas = append(bulkDatas, bulkData)
	}
	return obj.Bulk(ctx, bulkDatas...)
}
func (obj *Client) Update(ctx context.Context, updateDatas ...UpdateData) error {
	bulkDatas := []BulkData{}
	for _, data := range updateDatas {
		bulkData := BulkData{
			Method: Update,
			Index:  data.Index,
			Id:     data.Id,
			Data:   data.Data,
		}
		bulkDatas = append(bulkDatas, bulkData)
	}
	return obj.Bulk(ctx, bulkDatas...)
}
func (obj *Client) Upsert(ctx context.Context, updateDatas ...UpdateData) error {
	bulkDatas := []BulkData{}
	for _, data := range updateDatas {
		bulkData := BulkData{
			Method: Upsert,
			Index:  data.Index,
			Id:     data.Id,
			Data:   data.Data,
		}
		bulkDatas = append(bulkDatas, bulkData)
	}
	return obj.Bulk(ctx, bulkDatas...)
}
func (obj *Client) DeleteByQuery(ctx context.Context, index string, data any) error {
	href := obj.baseUrl + fmt.Sprintf("/%s/_delete_by_query", index)
	_, err := obj.search(ctx, "post", href, data)
	return err
}

type Method int

const (
	Update Method = iota
	Upsert
	Delete
	Insert
)

type BulkData struct {
	Method Method
	Index  string
	Id     string
	Data   any
}

func (obj *Client) Bulk(ctx context.Context, bulkDatas ...BulkData) error {
	if len(bulkDatas) == 0 {
		return nil
	}
	var body bytes.Buffer
	for _, bulkData := range bulkDatas {
		var method string
		switch bulkData.Method {
		case Update, Upsert:
			method = "update"
		}
		_, err := body.WriteString(fmt.Sprintf(`{"%s":{"_index":"%s","_id":"%s"}}`, method, bulkData.Index, bulkData.Id))
		if err != nil {
			return err
		}
		if bulkData.Data != nil {
			jsonData, err := gson.Decode(bulkData.Data)
			if err != nil {
				return err
			}
			tempBody := map[string]any{
				"doc": jsonData.Value(),
			}
			if bulkData.Method == Upsert {
				tempBody["doc_as_upsert"] = true
			}
			con, err := json.Marshal(tempBody)
			if err != nil {
				return err
			}
			_, err = body.WriteString("\n")
			if err != nil {
				return err
			}
			_, err = body.Write(con)
			if err != nil {
				return err
			}
		}
		_, err = body.WriteString("\n")
		if err != nil {
			return err
		}
	}
	_, err := obj.search(ctx, "post", obj.baseUrl+"/_bulk", body.Bytes())
	return err
}
func (obj *Client) search(ctx context.Context, method string, href string, body any) (jsonData *gson.Client, err error) {
	resp, err := obj.reqCli.Request(ctx, method, href, requests.RequestOption{
		Body: body,
		Headers: map[string]string{
			"Content-Type": "application/x-ndjson",
		},
	})
	if err != nil {
		return nil, err
	}
	jsonData, err = resp.Json()
	if err != nil {
		return
	}
	if e := jsonData.Get("error"); e.Exists() {
		err = errors.New(jsonData.String())
	}
	if jsonData.Get("errors").Bool() {
		err = errors.New(jsonData.String())
		if ierrs := jsonData.Get("items").Array(); len(ierrs) > 0 {
			ignoreError := true
			for _, ierr := range ierrs {
				errorType := ierr.Get("update.error.type").String()
				if errorType != "version_conflict_engine_exception" {
					ignoreError = false
					break
				}
			}
			if ignoreError {
				err = nil
			}
		}
	}
	return
}
func NewClient(ctx context.Context, option ClientOption) (*Client, error) {
	var client Client
	var err error
	if client.reqCli, err = requests.NewClient(ctx, requests.ClientOption{MaxRetries: 3, Proxy: option.Proxy}); err != nil {
		return nil, err
	}
	baseUrl, err := getBaseUrl(option)
	if err != nil {
		return nil, err
	}
	client.baseUrl = baseUrl
	return &client, client.Ping(ctx)
}
