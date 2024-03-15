package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/gospider007/gson"
	"github.com/gospider007/requests"
)

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

func (obj *Client) parseResponse(resp *requests.Response) (jsonData *gson.Client, err error) {
	jsonData, err = resp.Json()
	if err != nil {
		return
	}
	if e := jsonData.Get("error"); e.Exists() {
		err = errors.New(e.String())
	}
	if e := jsonData.Get("errors"); e.Exists() {
		err = errors.New(jsonData.Get("items").String())
	}
	return
}
func (obj *Client) Count(ctx context.Context, index string, data any) (int64, error) {
	url := obj.baseUrl + fmt.Sprintf("/%s/_count", index)
	rs, err := obj.reqCli.Request(ctx, "post", url, requests.RequestOption{Json: data})
	if err != nil {
		return 0, err
	}

	jsonData, err := obj.parseResponse(rs)
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
	url := obj.baseUrl + fmt.Sprintf("/%s/_search", index)
	rs, err := obj.reqCli.Request(ctx, "post", url, requests.RequestOption{Json: data})
	if err != nil {
		return searchResult, err
	}
	jsonData, err := obj.parseResponse(rs)
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
func (obj *Client) Exists(ctx context.Context, index, id string) (bool, error) {
	url := obj.baseUrl + fmt.Sprintf("/%s/_count?q=_id:%s", index, id)
	rs, err := obj.reqCli.Request(ctx, "get", url)
	if err != nil {
		return false, err
	}
	jsonData, err := obj.parseResponse(rs)
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
	return obj.deletes(ctx, deleteDatas...)
}
func (obj *Client) Update(ctx context.Context, updateDatas ...UpdateData) error {
	return obj.updates(ctx, false, updateDatas...)
}
func (obj *Client) Upsert(ctx context.Context, updateDatas ...UpdateData) error {
	return obj.updates(ctx, true, updateDatas...)
}
func (obj *Client) DeleteByQuery(ctx context.Context, index string, data any) error {
	url := obj.baseUrl + fmt.Sprintf("/%s/_delete_by_query", index)
	rs, err := obj.reqCli.Post(ctx, url, requests.RequestOption{Json: data})
	if err != nil {
		return err
	}
	_, err = obj.parseResponse(rs)
	return err
}
func (obj *Client) deletes(ctx context.Context, deleteDatas ...DeleteData) error {
	if len(deleteDatas) == 0 {
		return errors.New("no delete data")
	}
	var body bytes.Buffer
	for _, deleteData := range deleteDatas {
		_, err := body.WriteString(fmt.Sprintf(`{"delete":{"_index":"%s","_id":"%s"}}`, deleteData.Index, deleteData.Id))
		if err != nil {
			return err
		}
		_, err = body.WriteString("\n")
		if err != nil {
			return err
		}
	}
	url := obj.baseUrl + "/_bulk"
	rs, err := obj.reqCli.Request(ctx, "post", url, requests.RequestOption{
		Body: body.Bytes(),
		Headers: map[string]string{
			"Content-Type": "application/x-ndjson",
		},
	})
	if err != nil {
		return err
	}
	_, err = obj.parseResponse(rs)
	return err
}

//	func (obj *Client) update(ctx context.Context, updateData UpdateData, upsert bool) error {
//		jsonData, err := gson.Decode(updateData.Data)
//		if err != nil {
//			return err
//		}
//		body := map[string]any{
//			"doc": jsonData.Value(),
//		}
//		if upsert {
//			body["doc_as_upsert"] = true
//		}
//		url := obj.baseUrl + fmt.Sprintf("/%s/_update/%s", updateData.Index, updateData.Id)
//		rs, err := obj.reqCli.Request(ctx, "post", url, requests.RequestOption{Json: body})
//		if err != nil {
//			return err
//		}
//		_, err = obj.parseResponse(rs)
//		return err
//	}
func (obj *Client) updates(ctx context.Context, upsert bool, updateDatas ...UpdateData) error {
	if len(updateDatas) == 0 {
		return errors.New("no update data")
	}
	var body bytes.Buffer
	for _, updateData := range updateDatas {
		_, err := body.WriteString(fmt.Sprintf(`{"update":{"_index":"%s","_id":"%s"}}`, updateData.Index, updateData.Id))
		if err != nil {
			return err
		}
		jsonData, err := gson.Decode(updateData.Data)
		if err != nil {
			return err
		}
		tempBody := map[string]any{
			"doc": jsonData.Value(),
		}
		if upsert {
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
		_, err = body.WriteString("\n")
		if err != nil {
			return err
		}
	}
	url := obj.baseUrl + "/_bulk"
	rs, err := obj.reqCli.Request(ctx, "post", url, requests.RequestOption{
		Body: body.Bytes(),
		Headers: map[string]string{
			"Content-Type": "application/x-ndjson",
		},
	})
	if err != nil {
		return err
	}
	_, err = obj.parseResponse(rs)
	return err
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
