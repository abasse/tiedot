// Document management handlers.

package httpapi

import (
	"encoding/json"
	"fmt"
	"image/jpeg"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/nfnt/resize"
)

// CSDir contentstore directory
var CSDir string

type Filemeta struct {
	Id       int    `json:"id"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
	Filetype string `json:"filetype"`
}

// Insert a document into collection.
func Insert(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS")
	var col, doc string
	var hasfile bool

	if !Require(w, r, "col", &col) {
		return
	}

	//CS  Check for uploaded file
	file, handler, err := r.FormFile("file")

	if err == nil {
		defer file.Close()
		hasfile = true
		doc = r.FormValue("doc")
	} else {
		bodyBytes, _ := ioutil.ReadAll(r.Body)
		doc = string(bodyBytes)
		defer r.Body.Close()
	}

	if doc == "" && !Require(w, r, "doc", &doc) {
		return
	}
	var jsonDoc map[string]interface{}
	if err := json.Unmarshal([]byte(doc), &jsonDoc); err != nil {
		http.Error(w, fmt.Sprintf("'%v' is not valid JSON document.", doc), 400)
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}

	id, err := dbcol.Insert(jsonDoc)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}

	// CS Save incoming document to file system
	SaveDocument(id, col, ".json", doc)

	//CS save file to contentstore if exists
	if hasfile {
		// CS Create meta file
		fm := Filemeta{id, handler.Filename, handler.Size, handler.Header["Content-Type"][0]}
		filemeta, err := json.Marshal(fm)
		if err != nil {
			fmt.Println(err)
			return
		}

		//Create preview for jpeg
		if handler.Header["Content-Type"][0] == "image/jpeg" {
			CreatePreview(id, col, file)
		}

		SaveDocument(id, col, ".meta", string(filemeta))
		f, err := os.OpenFile(getPathToID(id, col)+filepath.Ext(handler.Filename), os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer f.Close()
		io.Copy(f, file)
	}

	w.WriteHeader(201)
	w.Write([]byte(fmt.Sprint(id)))
}

// Find and retrieve a document by ID.
func Get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS")
	var col, id string

	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "id", &id) {
		return
	}
	docID, err := strconv.Atoi(id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid document ID '%v'.", id), 400)
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	doc, err := dbcol.Read(docID)
	if doc == nil {
		http.Error(w, fmt.Sprintf("No such document ID %d.", docID), 404)
		return
	}
	resp, err := json.Marshal(doc)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
	w.Write(resp)
}

// Divide documents into roughly equally sized pages, and return documents in the specified page.
func GetPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS")
	var col, page, total string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "page", &page) {
		return
	}
	if !Require(w, r, "total", &total) {
		return
	}
	totalPage, err := strconv.Atoi(total)
	if err != nil || totalPage < 1 {
		http.Error(w, fmt.Sprintf("Invalid total page number '%v'.", totalPage), 400)
		return
	}
	pageNum, err := strconv.Atoi(page)
	if err != nil || pageNum < 0 || pageNum >= totalPage {
		http.Error(w, fmt.Sprintf("Invalid page number '%v'.", page), 400)
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	docs := make(map[string]interface{})
	dbcol.ForEachDocInPage(pageNum, totalPage, func(id int, doc []byte) bool {
		var docObj map[string]interface{}
		if err := json.Unmarshal(doc, &docObj); err == nil {
			docs[strconv.Itoa(id)] = docObj
		}
		return true
	})
	resp, err := json.Marshal(docs)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
	w.Write(resp)
}

// Update a document.
func Update(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS")
	var col, id, doc string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "id", &id) {
		return
	}
	defer r.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(r.Body)
	doc = string(bodyBytes)
	if doc == "" && !Require(w, r, "doc", &doc) {
		return
	}
	docID, err := strconv.Atoi(id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid document ID '%v'.", id), 400)
		return
	}
	var newDoc map[string]interface{}
	if err := json.Unmarshal([]byte(doc), &newDoc); err != nil {
		http.Error(w, fmt.Sprintf("'%v' is not valid JSON document.", newDoc), 400)
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	err = dbcol.Update(docID, newDoc)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}

	// Update document in contentstore
	DeleteDocument(docID, col)
	SaveDocument(docID, col, ".json", doc)

}

// Delete a document.
func Delete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS")
	var col, id string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "id", &id) {
		return
	}
	docID, err := strconv.Atoi(id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid document ID '%v'.", id), 400)
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	dbcol.Delete(docID)

	//Delete document from contentstore
	DeleteDocument(docID, col)

}

// Return approximate number of documents in the collection.
func ApproxDocCount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS")
	var col string
	if !Require(w, r, "col", &col) {
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	w.Write([]byte(strconv.Itoa(dbcol.ApproxDocCount())))
}

// CSGet retrieve contentstore document by ID.
func CSGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS")
	var id, col, ftype string

	if !Require(w, r, "id", &id) {
		return
	}

	if !Require(w, r, "col", &col) {
		return
	}

	if len(id) < 4 {
		return
	}

	if !Require(w, r, "type", &ftype) {
		return
	}

	if ftype == "json" || ftype == "meta" {
		w.Header().Set("Content-Type", "application/json")
	} else if ftype == "jpg" || ftype == "preview.jpg" {
		w.Header().Set("Content-Type", "image/jpeg")
	}

	docID, err := strconv.Atoi(id)
	check(err)

	if !fileExists(getPathToID(docID, col) + "." + ftype) {
		http.Error(w, fmt.Sprintf("File for id '%s' does not exist", id), 400)
		return
	}

	doc, err := ioutil.ReadFile(getPathToID(docID, col) + "." + ftype)
	check(err)

	w.Write(doc)
}

// CreatePreview
func CreatePreview(id int, col string, file multipart.File) {
	img, err := jpeg.Decode(file)
	if err != nil {
		log.Fatal(err)
	}
	file.Close()

	// resize to width 150 using Lanczos resampling
	// and preserve aspect ratio
	m := resize.Resize(150, 0, img, resize.Lanczos3)

	out, err := os.Create(getPathToID(id, col) + ".preview.jpg")
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	// write new image to file
	jpeg.Encode(out, m, nil)
}

// SaveDocument saves document to filesystem
func SaveDocument(id int, col string, ext string, content string) {
	path := CSDir + "/" + col + "/" + strconv.Itoa(id)[0:2] + "/" + strconv.Itoa(id)[2:4]

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(path, 0777)
	}

	f, err := os.Create(getPathToID(id, col) + ext)
	check(err)
	_, err = f.WriteString(content)
	check(err)

	return
}

// DeleteDocument saves document to filesystem
func DeleteDocument(id int, col string) {

	files, err := filepath.Glob(getPathToID(id, col) + "*")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

	// if fileExists(getPathToID(id, col) + ext) {
	// 	var err = os.Remove(getPathToID(id, col) + ext)
	// 	check(err)
	// }
	return
}

func getPathToID(id int, col string) string {
	return CSDir + "/" + col + "/" + strconv.Itoa(id)[0:2] + "/" + strconv.Itoa(id)[2:4] + "/" + strconv.Itoa(id)
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// SetCSDir set contentstore directory
func SetCSDir(dir string) {
	CSDir = dir
}
