package stream_upload

import (
	"bytes"
	"errors"
	"fmt"
	dgctx "github.com/darwinOrg/go-common/context"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
)

type stepEnum int

var LoggerFunc func(dc *dgctx.DgContext, message string)

const (
	readFirstSegStep    = stepEnum(0)
	readFileContentStep = stepEnum(1)
	readLastSegStep     = stepEnum(2)
	//readFinish          = stepEnum(-1)
)

const fileReaderBuffSize = 1024 * 16

type streamUpload struct {
	file     *os.File
	firstSeg bytes.Buffer
	lastSeg  bytes.Buffer
	step     stepEnum

	out        *os.File
	outLen     int
	outFileLen int
	debugMode  bool

	dc *dgctx.DgContext

	debugOutFileName string
}

type UpDebugInfo struct {
	DebugBoundary string
	DebugOutFile  string
}

func (sup *streamUpload) Close() error {
	if sup.file != nil {
		err := sup.file.Close()
		sup.file = nil
		return err
	}
	return nil
}

func (sup *streamUpload) addFile(fileParamName string, filename string, fieldParams map[string]string, debugInfo *UpDebugInfo) (string, error) {
	var err error
	if sup.file, err = os.Open(filename); err != nil {
		return "", err
	}
	w := multipart.NewWriter(&sup.firstSeg)
	if debugInfo != nil && len(debugInfo.DebugBoundary) > 0 {
		w.SetBoundary(debugInfo.DebugBoundary)
	}
	if debugInfo != nil && len(debugInfo.DebugOutFile) > 0 {
		sup.debugMode = true
		sup.debugOutFileName = debugInfo.DebugOutFile
	}
	_, err = w.CreateFormFile(fileParamName, filepath.Base(filename))
	if err != nil {
		sup.Close()
		return "", err
	}
	formDataContentType := w.FormDataContentType()
	boundary := w.Boundary()

	w = multipart.NewWriter(&sup.lastSeg)
	w.SetBoundary(boundary)
	if len(fieldParams) > 0 {
		sup.lastSeg.WriteString("\r\n")
	}
	for key, val := range fieldParams {
		err = w.WriteField(key, val)
		if err != nil {
			sup.Close()
			return "", err
		}
	}
	w.Close()
	if LoggerFunc != nil {
		msg := fmt.Sprintf("last:%s", sup.lastSeg.String())
		LoggerFunc(sup.dc, msg)
	}
	return formDataContentType, err
}

func (sup *streamUpload) toDebugFile(p []byte, n int) {
	if !sup.debugMode {
		return
	}
	var err error
	if sup.out == nil {
		sup.out, err = os.OpenFile(sup.debugOutFileName+".fin", os.O_CREATE|os.O_RDWR, 0644)
		if LoggerFunc != nil {
			LoggerFunc(sup.dc, fmt.Sprintf("open debugOutFileName: %v", err))
		}
	}
	if sup.out != nil {
		l, err := sup.out.Write(p[:n])
		sup.outLen += l

		if LoggerFunc != nil {
			LoggerFunc(sup.dc, fmt.Sprintf("write out data,size:%d,%v", l, err))
		}
	}
}

func (sup *streamUpload) closeDebugOutFile() {
	if sup.out != nil {
		sup.out.Close()
		if LoggerFunc != nil {
			msg := fmt.Sprintf("close out, and all len%d,file len:%d", sup.outLen, sup.outFileLen)
			LoggerFunc(sup.dc, msg)
		}
	}
}

func (sup *streamUpload) Read(p []byte) (n int, err error) {
	bufLen := len(p)
	if sup.debugMode && LoggerFunc != nil {
		LoggerFunc(sup.dc, fmt.Sprintf("buff size is:%d", bufLen))
	}
	gotLen := 0
	buff := p
	if sup.step == readFirstSegStep {
		n, err = sup.firstSeg.Read(buff)
		if err != nil {
			return n, err
		}
		if n > 0 {
			gotLen += n
			if gotLen == bufLen {
				sup.toDebugFile(p, gotLen)
				return gotLen, nil
			}
			sup.step = readFileContentStep
		} else {
			sup.step = readFileContentStep
		}
	}

	if gotLen > 0 {
		buff = p[gotLen:bufLen]
	}
	if sup.step == readFileContentStep {
		for {
			n, err = sup.file.Read(buff)
			if err != nil && err != io.EOF {
				return 0, err
			}
			if n > 0 {
				gotLen += n
				sup.outFileLen += n
				buff = p[gotLen:bufLen]
			}

			if err == io.EOF {
				sup.step = readLastSegStep
				break
			}
			if gotLen == bufLen {
				break
			}
		}

		if gotLen == bufLen {
			sup.toDebugFile(p, gotLen)
			return gotLen, err
		}
	}

	if gotLen > 0 {
		buff = p[gotLen:bufLen]
	}

	if sup.step == readLastSegStep {
		n, err = sup.lastSeg.Read(buff)
		if err != nil && err != io.EOF {
			return n, err
		}
		if n > 0 {
			gotLen += n
		}
		sup.toDebugFile(p, gotLen)
		if gotLen < bufLen {
			sup.closeDebugOutFile()
			return gotLen, io.EOF
		}
		if err == io.EOF {
			sup.closeDebugOutFile()
		}
		return gotLen, err
	}
	return 0, errors.New("met error")
}
func NewStreamFileUploadBody(fileParamName string, filename string, fieldParams map[string]string, traceId string, debugInfo *UpDebugInfo) (body io.ReadCloser, contentType string, err error) {
	up := &streamUpload{}
	up.dc = &dgctx.DgContext{
		TraceId: traceId,
	}
	contentType, err = up.addFile(fileParamName, filename, fieldParams, debugInfo)
	if err != nil {
		return nil, "", err
	}
	return up, contentType, nil
}

func NewFileUploadRequest(uri string, fieldParams map[string]string, fileParamName, filename string, traceId string, debugInfo *UpDebugInfo) (*http.Request, error) {
	body, contextType, err := NewStreamFileUploadBody(fileParamName, filename, fieldParams, traceId, debugInfo)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", uri, body)
	if err != nil {
		body.Close()
		return nil, err
	}
	req.Header.Set("Content-Type", contextType)
	return req, nil
}
