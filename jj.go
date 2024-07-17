package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net"
	"strings"
)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	counter := 0

	for {
		con, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}

		counter++

		if counter > 4 {

			break
		}

		go Send(con, counter)
	}

	select {}
}

func Send(con net.Conn, sender int) {

	defer con.Close()

	scanner := bufio.NewScanner(con)
	for scanner.Scan() {
		message := scanner.Text()
		if sender == 1 {
			fmt.Println("sandy ")
		} else if sender == 2 {
			fmt.Println("Bota ")
		} else if sender == 3 {
			fmt.Println("mary ")
		} else {
			fmt.Println("marco ")
		}

		db, err := sql.Open("mysql", "root:thomas78@tcp(127.0.0.1:3306)/project2")
		if err != nil {
			fmt.Fprintln(con, err)
		} else {
			if strings.HasPrefix(message, "select") || strings.HasPrefix(message, "SELECT") {
				Select(db, message, con)
			} else if strings.HasPrefix(message, "insert") || strings.HasPrefix(message, "INSERT") {
				insert(db, message, con)
			} else if strings.HasPrefix(message, "delete") || strings.HasPrefix(message, "DELETE") {
				delete(db, message, con)

			} else if strings.HasPrefix(message, "update") || strings.HasPrefix(message, "UPDATE") {
				update(db, message, con)
			} else {
				fmt.Fprintln(con, "syntax error")
			}

		}

	}

	go receiveMessages(con)

}

func receiveMessages(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
func Select(db *sql.DB, message string, conn net.Conn) {
	var (
		id       int
		name     string
		age      int
		sub_name string
		sub_id   int
		s_id     int
	)
	query, err := db.Query(message)
	if err != nil {
		fmt.Fprintln(conn, err)
	} else if strings.Contains(message, "*") && !strings.Contains(message, "subjects") {

		for query.Next() {

			err := query.Scan(&id, &name, &age)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, id, name, age)
		}

	} else if strings.Contains(message, "*") && !strings.Contains(message, "student") {

		for query.Next() {

			err := query.Scan(&sub_id, &sub_name, &s_id)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, sub_id, sub_name, s_id)
		}

	} else if strings.Contains(message, "*") {

		for query.Next() {

			err := query.Scan(&sub_id, &sub_name, &s_id, &id, &name, &age)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, sub_id, sub_name, s_id, id, name, age)
		}
	} else if !strings.Contains(message, "id =") && strings.Contains(message, "id") && !strings.Contains(message, "subjects") {
		for query.Next() {

			err := query.Scan(&id)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, id)
		}

	} else if strings.Contains(message, "select name from") && !strings.Contains(message, "name =") && !strings.Contains(message, "subjects") {
		println("pp")
		for query.Next() {

			err := query.Scan(&name)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, name)
		}

	} else if strings.Contains(message, "select age from ") && !strings.Contains(message, "age =") && !strings.Contains(message, "subjects") {

		for query.Next() {

			err := query.Scan(&age)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, age)
		}

	} else if strings.Contains(message, "age,name") && strings.Contains(message, "id =") {

		for query.Next() {

			err := query.Scan(&age, &name)
			if err != nil {
				fmt.Fprintln(conn, err)
			}

		}
		fmt.Fprintln(conn, age, name)
	} else if strings.Contains(message, "name,age") && strings.Contains(message, "id =") {
		for query.Next() {

			err := query.Scan(&name, &age)
			if err != nil {
				fmt.Fprintln(conn, err)
			}

		}
		fmt.Fprintln(conn, name, age)
	} else if strings.Contains(message, "name,age") && !strings.Contains(message, "id =") {
		for query.Next() {

			err := query.Scan(&name, &age)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, name, age)
		}

	} else if strings.Contains(message, "age,name") && !strings.Contains(message, "id =") {
		for query.Next() {

			err := query.Scan(&age, &name)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, age, name)
		}

	} else if !strings.Contains(message, "sub_id =") && strings.Contains(message, "sub_id") && !strings.Contains(message, "student") {
		for query.Next() {

			err := query.Scan(&sub_id)
			if err != nil {
				fmt.Fprintln(conn, err)
			}

		}
		fmt.Fprintln(conn, sub_id)
	} else if strings.Contains(message, "select sub_name from") && !strings.Contains(message, "sub_name =") && !strings.Contains(message, "student") {
		for query.Next() {

			err := query.Scan(&sub_name)
			if err != nil {
				fmt.Fprintln(conn, err)
			}

		}
		fmt.Fprintln(conn, sub_name)
	} else if strings.Contains(message, "select id from") && !strings.Contains(message, "id =") && !strings.Contains(message, "student") {
		for query.Next() {

			err := query.Scan(&s_id)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, s_id)
		}

	} else if strings.Contains(message, "id,sub_name") && strings.Contains(message, "sub_id =") {
		for query.Next() {

			err := query.Scan(&s_id, &sub_name)
			if err != nil {
				fmt.Fprintln(conn, err)
			}

		}
		fmt.Fprintln(conn, s_id, sub_name)
	} else if strings.Contains(message, "sub_name,id") && strings.Contains(message, "sub_id =") {
		for query.Next() {

			err := query.Scan(&sub_name, &s_id)
			if err != nil {
				fmt.Fprintln(conn, err)
			}

		}
		fmt.Fprintln(conn, sub_name, s_id)
	} else if strings.Contains(message, "sub_name,id") && !strings.Contains(message, "sub_id =") {
		for query.Next() {

			err := query.Scan(&sub_name, &s_id)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, sub_name, s_id)
		}

	} else if strings.Contains(message, "id,sub_name") && !strings.Contains(message, "sub_id =") {
		for query.Next() {

			err := query.Scan(&s_id, &sub_name)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, s_id, sub_name)
		}

	} else if strings.Contains(message, "name,sub_name") && !strings.Contains(message, "id =") {
		for query.Next() {

			err := query.Scan(&name, &sub_name)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, name, sub_name)
		}

	} else if strings.Contains(message, "sub_name,name") && !strings.Contains(message, "id =") {
		for query.Next() {

			err := query.Scan(&sub_name, &name)
			if err != nil {
				fmt.Fprintln(conn, err)
			}
			fmt.Fprintln(conn, sub_name, name)
		}

	} else if strings.Contains(message, "sub_name,name") && strings.Contains(message, "id =") {
		for query.Next() {

			err := query.Scan(&sub_name, &name)
			if err != nil {
				fmt.Fprintln(conn, err)
			}

		}
		fmt.Fprintln(conn, sub_name, name)

	} else if strings.Contains(message, "name,sub_name") && strings.Contains(message, "id =") {
		for query.Next() {

			err := query.Scan(&name, &sub_name)
			if err != nil {
				fmt.Fprintln(conn, err)
			}

		}
		fmt.Fprintln(conn, name, sub_name)

	}
}
func insert(db *sql.DB, message string, conn net.Conn) {
	_, err := db.Query(message)
	if err != nil {
		fmt.Fprintln(conn, err)
	} else {
		fmt.Fprintln(conn, "insertion is done")
	}

}
func delete(db *sql.DB, message string, conn net.Conn) {
	_, err := db.Query(message)
	if err != nil {
		fmt.Fprintln(conn, err)
	} else {
		fmt.Fprintln(conn, "deletion is done")
	}

}
func update(db *sql.DB, message string, conn net.Conn) {
	_, err := db.Query(message)
	if err != nil {
		fmt.Fprintln(conn, err)
	} else {
		fmt.Fprintln(conn, "update is done")
	}

}
