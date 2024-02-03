package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"net/http"
	"strconv"
	"time"
)

const NumberOfRequestedNews = 5

type NewsletterNewsItem struct {
	ArticleURL        string `bson:"article_url" xml:"ArticleURL"`
	NewsArticleID     int    `bson:"news_article_id" xml:"NewsArticleID"`
	PublishDate       string `bson:"publish_date" xml:"PublishDate"`
	Taxonomies        string `bson:"taxonomies" xml:"Taxonomies"`
	TeaserText        string `bson:"teaser_text" xml:"TeaserText"`
	ThumbnailImageURL string `bson:"thumbnail_image_url" xml:"ThumbnailImageURL"`
	Title             string `bson:"title" xml:"Title"`
	OptaMatchId       string `bson:"opta_match_id" xml:"OptaMatchId"`
	LastUpdateDate    string `bson:"last_update_date" xml:"LastUpdateDate"`
	IsPublished       string `bson:"is_published" xml:"IsPublished"`
}

type NewsletterNewsItems struct {
	XMLName             xml.Name             `xml:"NewListInformation"`
	ClubName            string               `xml:"ClubName"`
	ClubWebsiteURL      string               `xml:"ClubWebsiteURL"`
	NewsletterNewsItems []NewsletterNewsItem `xml:"NewsletterNewsItems>NewsletterNewsItem"`
}

type Repository struct {
	client *mongo.Client
}

type Cron struct {
	client     *resty.Client
	repository *Repository
}

func main() {
	repository := NewRepository()
	NewCron(repository).Run(time.Second * 3)
	r := gin.Default()

	r.GET("/all-news", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": repository.GetAllNews(),
		})
	})

	r.GET("/news", func(c *gin.Context) {
		id, err := strconv.Atoi(c.Query("id"))
		if err != nil {
			c.String(http.StatusBadRequest, "некорректные данные")
			return
		}
		result, ok := repository.GetNewsByID(id)
		if ok {
			c.JSON(http.StatusOK, gin.H{
				"message": result,
			})
			return
		} else {
			c.String(http.StatusNotFound, "запись не найдена")
			return
		}
	})

	err := r.Run(":8080")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err := repository.client.Disconnect(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Connection to MongoDB closed.")
	}()
}

func (r *Repository) InsertManyNews(news []NewsletterNewsItem) {
	collection := r.client.Database("mydatabase").Collection("news")
	n := make([]interface{}, len(news))
	for i, person := range news {
		n[i] = person
	}
	_, err := collection.InsertMany(context.Background(), n)
	if err != nil {
		log.Println(err)
	}
}

func (r *Repository) GetAllNews() []NewsletterNewsItem {
	collection := r.client.Database("mydatabase").Collection("news")
	cursor, err := collection.Find(context.Background(), bson.M{})
	if err != nil {
		log.Println(err)
	}

	defer func(cursor *mongo.Cursor, ctx context.Context) {
		err := cursor.Close(ctx)
		if err != nil {
			log.Println(err)
		}
	}(cursor, context.Background())

	var results []NewsletterNewsItem

	if err = cursor.All(context.Background(), &results); err != nil {
		log.Println(err)
	}

	if err = cursor.Err(); err != nil {
		log.Println(err)
	}

	return results
}

func (r *Repository) GetNewsByID(id int) (NewsletterNewsItem, bool) {
	collection := r.client.Database("mydatabase").Collection("news")

	var result NewsletterNewsItem

	err := collection.FindOne(context.Background(), bson.M{"news_article_id": id}).Decode(&result)
	fmt.Println(result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.Println("Запись не найдена")
			return NewsletterNewsItem{}, false
		}
	}

	return result, true
}

func (r *Repository) GetNewsWithMaxDate() (NewsletterNewsItem, bool) {
	var result NewsletterNewsItem
	collection := r.client.Database("mydatabase").Collection("news")

	pipeline := []bson.M{
		{
			"$sort": bson.M{"last_update_date": -1},
		},
		{
			"$limit": 1,
		},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}

	defer func(cursor *mongo.Cursor, ctx context.Context) {
		err = cursor.Close(ctx)
		if err != nil {
			log.Println(err)
		}
	}(cursor, context.Background())

	if cursor.Next(context.Background()) {
		if err = cursor.Decode(&result); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Println("Запись не найдена")
		return result, false
	}

	return result, true
}

func NewRepository() *Repository {
	clientOptions := options.Client().ApplyURI("mongodb://mongoadmin:bdung@localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	return &Repository{client}
}

func (c *Cron) Run(duration time.Duration) {
	go func() {
		for {
			url := fmt.Sprintf("https://www.htafc.com/api/incrowd/getnewlistinformation?count=%d", NumberOfRequestedNews)
			response, err := c.client.R().Get(url)
			if err != nil {
				log.Println("Ошибка при выполнении GET-запроса:", err)
				continue
			}

			var newsItems NewsletterNewsItems
			newItemsNewsToInsert := make([]NewsletterNewsItem, 0, NumberOfRequestedNews)
			err = xml.Unmarshal(response.Body(), &newsItems)
			if err != nil {
				log.Println("Ошибка при разборе XML:", err)
				continue
			}

			news, ok := c.repository.GetNewsWithMaxDate()

			if ok {
				for _, item := range newsItems.NewsletterNewsItems {
					date1, err := c.formattingDate(news.LastUpdateDate)
					if err != nil {
						continue
					}

					date2, err := c.formattingDate(item.LastUpdateDate)
					if err != nil {
						continue
					}

					if date1.Before(date2) { // если date2 больше date1
						newItemsNewsToInsert = append(newItemsNewsToInsert, item)
					}

				}
				if len(newItemsNewsToInsert) > 0 { // проверка на то что среди новостей из запроса удалось найти хотя бы одну новую новость
					c.repository.InsertManyNews(newItemsNewsToInsert)
				}
			} else {
				c.repository.InsertManyNews(newsItems.NewsletterNewsItems) // когда база пустая и не удалось достать max дату
			}

			time.Sleep(duration)
		}
	}()
}

func (*Cron) formattingDate(date string) (time.Time, error) {
	dateFormatted, err := time.Parse("2006-01-02 15:04:05", date)
	if err != nil {
		return time.Time{}, err
	}

	return dateFormatted, nil
}

func NewCron(repository *Repository) *Cron {
	return &Cron{
		resty.New(),
		repository,
	}
}
