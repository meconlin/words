// Package words provides data access methods for Word objects.
//
package words

import (
	"database/sql"
	"github.com/coopernurse/gorp"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
)

var logger *log.Logger
var dbmap *gorp.DbMap

// Word represents a word and its count in the datastore.
type Word struct {
	Word  string `json:"word" binding:"required" db:"word"`
	Count int    `json:"count" db:"count"`
}

func init() {
	dbmap = initDb()
	logger = log.New(os.Stderr, "[words] ", log.Lshortfile)
}

// WordUpsert upserts a Word in the datastore.
// An upsert will insert a new word with a count = 1 OR update an existing word with count = count + 1.
func WordUpsert(word Word) error {
	existingWord := Word{}
	err := dbmap.SelectOne(&existingWord, "select * from words where word=?", word.Word)

	if err == sql.ErrNoRows {
		// new create
		word.Count = 1
		err = dbmap.Insert(&word)
	} else if err == nil {
		// update
		existingWord.Count = existingWord.Count + 1
		_, err = dbmap.Update(&existingWord)
		checkErr(err, "Update failed")
	}

	return err
}

// FetchWords gets all Words from the datastore.
// If none present an empty slice is returned.
func FetchWords() ([]Word, error) {
	var words []Word
	_, err := dbmap.Select(&words, "select * from words")
	checkErr(err, "FetchWords : select failed")
	return words, err
}

// FetchWord gets a Word from the datastore.
// If not present an empty Word object is retruned.
func FetchWord(w string) (Word, error) {
	word := Word{}
	err := dbmap.SelectOne(&word, "select * from words where word=?", w)

	// not found
	if err == sql.ErrNoRows {
		return Word{}, nil
		//other error
	} else if err != nil {
		return Word{}, err
	}

	return word, nil
}

func initDb() *gorp.DbMap {
	// connect to db using standard Go database/sql API
	// use whatever database/sql driver you wish
	db, err := sql.Open("sqlite3", "/tmp/api_db.bin")
	checkErr(err, "sql.Open failed")

	// construct a gorp DbMap
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}

	// add table words, with unique word col
	dbmap.AddTableWithName(Word{}, "words").SetKeys(false, "word")

	// truncate table, we are just jacking around, so kill it in between runs
	dbmap.TruncateTables()

	// create the table. in a production system you'd generally
	// use a migration tool, or create the tables via scripts
	err = dbmap.CreateTablesIfNotExists()
	checkErr(err, "Create tables failed")

	return dbmap
}

func checkErr(err error, msg string) {
	if err != nil {
		logger.Fatalln(msg, err)
	}
}
