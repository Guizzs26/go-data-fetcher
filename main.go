/*

wg *sync.WaitGroup
- It's a "counter" of active goroutines;
- A pointer is passed so that all concurrent functions share the same sync control;
- The function that calls fetchPosts() will do wg.Add(1) - BEFORE calling it;
	- The number of registered goroutines is done with .Add(n).
- Inside the func, we use defer wg.Done() to inform that the goroutine is finished;

! Defer it ensures that even if an error or early return occurs, Done() will be called.

*/

/*

xCh chan<- []X;
- This is a UNIDIRECTIONAL channel that accepts data from []X (writing data to channel).
- Anyone who calls fetchPosts() will pass this channel and wait for the data to return;
- This allows data to be passed from one goroutine to another in a safe and concurrent manner.

*/

// defer res.Body.Close() ensures that the connection will be closed correctly.

/*

Decoder
- First creates a Post slice (the API JSON is an array of objects);
- It uses json.NewDecoder which is more performant and straightforward than ReadAll + Unmarshal;
- Fills the slice with data from the response body.

*/

/*

postsCh <- posts
- Sends the Post slice through the postsCh channel.
- The goroutine that called fetchPosts() can receive this data and continue the flow.

! This send blocks the goroutine until someone is ready to receive it, which is important to
understand to avoid deadlocks.

*/

/*

Why's?

1 - One channel for each type

1.1. Separation of responsibilities and types

Each channel carries data of a specific type:

userCh chan []User
postCh chan []Post
commentCh chan []Comment
If you used a single channel for all, you would have to:

Use interface{} (loses the type, requires type assertions later)

Add a "type" field in a generic struct to identify what is arriving

Handle more complex logic (switches, casts)

--- // ---

1.2. Isolated and conflict-free concurrency

Each fetcher (fetchPosts, fetchUsers, fetchComments) works independently,
without conflicting writes on the channel.

If they all wrote to the same channel:
- They would have to do some kind of extra synchronization (mutex, struct with type, etc.)
- They could compete for sending and generate bugs or exchanged messages

With separate channels, you ensure that:

✅ Each data type has its own communication path
✅ Each goroutine writes to its own channel, without blocking each other

--- // ---

1.3. Makes it easier to read and control the orchestration

In the fetchAllData function, you can do:

users := <-userCh
posts := <-postCh
comments := <-commentCh

Simple, clear and predictable.

If you used a single channel, you would have to:
- Use select or for with interface{} to find out who sent what
- Build extra logic to identify and order the data

This would make the function much less readable and more prone to bugs.

--- // ---

1.4. Type decoupling

By using separate channels, your code is prepared to grow:

If you want to handle an error of one type (e.g. fetchComments failed),
you only deal with its channel

If you want to do retry, timeout or circuit breaker in one fetcher, it
doesn't interfere with the others

*/

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

var (
	GET_USERS_URL    = "https://jsonplaceholder.typicode.com/users"
	GET_POSTS_URL    = "https://jsonplaceholder.typicode.com/posts"
	GET_COMMENTS_URL = "https://jsonplaceholder.typicode.com/comments"

	filename = "data.json"
)

type User struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Username string `json:"username"`
	Email    string `json:"email"`
}

type Post struct {
	ID     int    `json:"id"`
	UserID int    `json:"userId"`
	Title  string `json:"title"`
	Body   string `json:"body"`
}

type Comment struct {
	ID     int    `json:"id"`
	PostID int    `json:"postId"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Body   string `json:"body"`
}

type UserWithPostsAndComments struct {
	User  User               `json:"user"`
	Posts []PostWithComments `json:"posts"`
}

type PostWithComments struct {
	Post    Post      `json:"post"`
	Comment []Comment `json:"comment"`
}

/*
We want something like that:

[

	{
	  "user": {...},
	  "posts": [
	    {
	      "post": {...},
	      "comments": [...]
	    },
	    ...
	  ]
	},
	...

]
*/
func AggregateData(
	users []User,
	posts []Post,
	comments []Comment,
) []UserWithPostsAndComments {
	postMap := make(map[int][]Post)
	for _, p := range posts {
		postMap[p.UserID] = append(postMap[p.UserID], p)
	}

	commentMap := make(map[int][]Comment)
	for _, c := range comments {
		commentMap[c.PostID] = append(commentMap[c.PostID], c)
	}

	var result []UserWithPostsAndComments
	for _, u := range users {
		userPosts := postMap[u.ID]

		var postWithComments []PostWithComments
		for _, p := range userPosts {
			postWithComments = append(postWithComments, PostWithComments{
				Post:    p,
				Comment: commentMap[p.ID],
			})
		}

		result = append(result, UserWithPostsAndComments{
			User:  u,
			Posts: postWithComments,
		})
	}
	return result
}

func fetchUsers(wg *sync.WaitGroup, usersCh chan<- []User) {
	defer wg.Done()

	res, err := http.Get(GET_USERS_URL)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()

	var users []User
	if err := json.NewDecoder(res.Body).Decode(&users); err != nil {
		log.Fatal(err)
	}

	usersCh <- users
}

func fetchPosts(wg *sync.WaitGroup, postsCh chan<- []Post) {
	defer wg.Done()

	res, err := http.Get(GET_POSTS_URL)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()

	var posts []Post
	if err := json.NewDecoder(res.Body).Decode(&posts); err != nil {
		log.Fatal(err)
	}

	postsCh <- posts
}

func fetchComments(wg *sync.WaitGroup, commentsCh chan<- []Comment) {
	defer wg.Done()

	res, err := http.Get(GET_COMMENTS_URL)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()

	var comments []Comment
	if err := json.NewDecoder(res.Body).Decode(&comments); err != nil {
		log.Fatal(err)
	}

	commentsCh <- comments
}

func fetchAllData() (users []User, posts []Post, comments []Comment) {
	// This wg will control the 3 goroutines: fetchUsers, fetchPosts and fetchComments.
	var wg sync.WaitGroup

	userCh := make(chan []User)
	postCh := make(chan []Post)
	commentCh := make(chan []Comment)

	/*
		It says: "I'm going to start 3 goroutines, wait for them all to finish."

		It's essential to do this before you start the goroutines.
	*/
	wg.Add(3)

	go fetchUsers(&wg, userCh)
	go fetchPosts(&wg, postCh)
	go fetchComments(&wg, commentCh)

	/*

		This anonymous function runs in parallel and:
		- Waits for all goroutines to finish with wg.Wait().
		- After that, it closes the channels — this is important to avoid deadlocks when receiving.

		! Closing the channel is not mandatory if you know that there will only be one send, but it
		is good practice when you receive with for range, for example.

	*/
	go func() {
		wg.Wait()

		close(userCh)
		close(postCh)
		close(commentCh)
	}()

	/*

		Here main() (or whoever calls fetchAllData) is blocked waiting for the data.

		When fetchX() finishes, it sends the data through the channel, and this function receives it.

	*/
	users = <-userCh
	posts = <-postCh
	comments = <-commentCh

	return
}

func writeToJSONFile(data []UserWithPostsAndComments, filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	_, err = file.Write(jsonData)
	if err != nil {
		return err
	}

	log.Println("✅ JSON file save successfully", filename)

	return nil
}

func readAndAnalyze(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	var users []UserWithPostsAndComments
	if err := json.Unmarshal(data, &users); err != nil {
		return err
	}

	totalUsers := len(users)
	totalPosts := 0
	totalComments := 0
	userMostPosts := ""
	maxPosts := 0

	for _, user := range users {
		numPosts := len(user.Posts)
		totalPosts += numPosts
		if numPosts > maxPosts {
			maxPosts = numPosts
			userMostPosts = user.User.Name
		}

		for _, post := range user.Posts {
			totalComments += len(post.Comment)
		}
	}

	fmt.Printf("Summary of data for file %q:\n", filename)
	fmt.Printf("Total users: %d\n", totalUsers)
	fmt.Printf("Total posts: %d\n", totalPosts)
	fmt.Printf("Total comments: %d\n", totalComments)
	fmt.Printf("User with most posts: %s (%d posts)\n", userMostPosts, maxPosts)

	return nil
}

func main() {
	users, posts, comments := fetchAllData()

	aggregated := AggregateData(users, posts, comments)

	if err := writeToJSONFile(aggregated, filename); err != nil {
		log.Fatal(err)
	}

	if err := readAndAnalyze(filename); err != nil {
		log.Fatalf("Falha ao processar o arquivo: %v", err)
	}
}
