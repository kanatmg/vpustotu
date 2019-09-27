package main

import (
	"container/heap"
	"flag"
	"fmt"
	"github.com/opesun/goquery"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
)

const (
	ENDMESSAGE = "SadButTrue"
)

//Вот так у нас теперь выглядит объявление переменных:
var (
	WORKERS     = 5     //количество рабочих
	WORKERSCAP  = 5     //размер очереди каждого рабочего
	ATATASTREAM = 291   //id потока ататы
	ATATAPOS    = 0     //стартовая позиция в потоке ататы
	IMGDIR      = "img" //директория для сохранения картинок
)

//Назначим флаги командной строки:
func init() {
	flag.IntVar(&WORKERS, "w", WORKERS, "количество рабочих")
	flag.IntVar(&ATATASTREAM, "s", ATATASTREAM, "id потока ататы")
	flag.IntVar(&ATATAPOS, "p", ATATAPOS, "стартовая позиция")
	flag.StringVar(&IMGDIR, "d", IMGDIR, "директория для картинок")
}

//Загрузка изображения
func download(url string) {
	response, err := http.Get(url)
	if err != nil {
		fmt.Println("Error while downloading", url, "-", err)
		return
	}
	fmt.Println(response.Body)
	file, err := os.Create("/home/kanat/nomer/asdf.jpg")
	//fileName := IMGDIR + "/" + url[strings.LastIndex(url, "/")+1:]
	//fmt.Println(fileName)
	//output, err := os.Create(file)
	//defer output.Close()


	writen,e := io.Copy(file, response.Body)
	fmt.Println(writen)
	if e != nil{
		fmt.Println("Error while copying", url, "-", e)
	}
	defer response.Body.Close()
}

type Worker struct {
	urls    chan string     // канал для заданий
	pending int             //кол-во оставшихся задач
	index   int             //позиция в куче
	wg      *sync.WaitGroup //указатель на группу ожидания
}

func (w *Worker) work(done chan *Worker) {
	for {
		url := <-w.urls //читаем следующее задание
		w.wg.Add(1)
		log.Println(url)
		download(url)
		w.wg.Done()
		done <- w
	}
}

//Это будет наша "куча":
type Pool []*Worker

//Проверка кто меньше - в нашем случае меньше тот у кого меньше заданий:
func (p Pool) Less(i, j int) bool { return p[i].pending < p[j].pending }

//Вернем количество рабочих в пуле:
func (p Pool) Len() int { return len(p) }

//Реализуем обмен местами:
func (p Pool) Swap(i, j int) {
	if i >= 0 && i < len(p) && j >= 0 && j < len(p) {
		p[i], p[j] = p[j], p[i]
		p[i].index, p[j].index = i, j
	}
}

//Заталкивание элемента:
func (p *Pool) Push(x interface{}) {
	n := len(*p)
	worker := x.(*Worker)
	worker.index = n
	*p = append(*p, worker)
}

//И выталкивание:
func (p *Pool) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	item.index = -1
	*p = old[0 : n-1]
	return item
}

//Балансировщик
type Balancer struct {
	pool     Pool            //Наша "куча" рабочих
	done     chan *Worker    //Канал уведомления о завершении для рабочих
	requests chan string     //Канал для получения новых заданий
	flowctrl chan bool       //Канал для PMFC
	queue    int             //Количество незавершенных заданий переданных рабочим
	wg       *sync.WaitGroup //Группа ожидания для рабочих
}

//Инициализируем балансировщик. Аргументом получаем канал по которому приходят задания
func (b *Balancer) init(in chan string) {
	b.requests = make(chan string)
	b.flowctrl = make(chan bool)
	b.done = make(chan *Worker)
	b.wg = new(sync.WaitGroup)

	//Запускаем наш Flow Control:
	go func() {
		for {
			b.requests <- <-in //получаем новое задание и пересылаем его на внутренний канал
			<-b.flowctrl       //а потом ждем получения подтверждения
		}
	}()

	//Инициализируем кучу и создаем рабочих:
	heap.Init(&b.pool)
	for i := 0; i < WORKERS; i++ {
		w := &Worker{
			urls:    make(chan string, WORKERSCAP),
			index:   0,
			pending: 0,
			wg:      b.wg,
		}
		go w.work(b.done)     //запускаем рабочего
		heap.Push(&b.pool, w) //и заталкиваем его в кучу
	}
}

// Функция отправки задания
func (b *Balancer) dispatch(url string) {
	w := heap.Pop(&b.pool).(*Worker) //Берем из кучи самого незагруженного рабочего..
	w.urls <- url                    //..и отправляем ему задание.
	w.pending++                      //Добавляем ему "весу"..
	heap.Push(&b.pool, w)            //..и отправляем назад в кучу
	if b.queue++; b.queue < WORKERS*WORKERSCAP {
		b.flowctrl <- true
	}
}

//Обработка завершения задания
func (b *Balancer) completed(w *Worker) {
	w.pending--
	heap.Remove(&b.pool, w.index)
	heap.Push(&b.pool, w)
	if b.queue--; b.queue == WORKERS*WORKERSCAP-1 {
		b.flowctrl <- true
	}
}
func (b *Balancer) balance(quit chan bool) {
	lastjobs := false //Флаг завершения, поднимаем когда кончились задания
	for {
		select { //В цикле ожидаем коммуникации по каналам:

		case <-quit: //пришло указание на остановку работы
			b.wg.Wait()  //ждем завершения текущих загрузок рабочими..
			quit <- true //..и отправляем сигнал что закончили

		case url := <-b.requests: //Получено новое задание (от flow controller)
			if url != ENDMESSAGE { //Проверяем - а не кодовая ли это фраза?
				b.dispatch(url) // если нет, то отправляем рабочим
			} else {
				lastjobs = true //иначе поднимаем флаг завершения
			}

		case w := <-b.done: //пришло уведомление, что рабочий закончил загрузку
			b.completed(w) //обновляем его данные
			if lastjobs {
				if w.pending == 0 { //если у рабочего кончились задания..
					heap.Remove(&b.pool, w.index) //то удаляем его из кучи
				}
				if len(b.pool) == 0 { //а если куча стала пуста
					//значит все рабочие закончили свои очереди
					quit <- true //и можно отправлять сигнал подтверждения готовности к останову
				}
			}
		}
	}
}

func generator(out chan string, stream, start int) {
	for position := start; ; position++ {
		html, err := goquery.ParseUrl("http://avto-nomer.ru/ru/gallery-" + strconv.Itoa(position) + "")
		fmt.Println("page-", position)
		if err == nil {
			for _, url := range html.Find("div.panel.panel-grey div.panel-body div.row a img").Attrs("src") {
				//fmt.Println(url)
				out <- url
			}
		} else {
			out <- ENDMESSAGE
			return
		}
	}
}

func main() {
	//разберем флаги
	flag.Parse()
	//создадим директорию для загрузки, если её еще нет
	if err := os.MkdirAll(IMGDIR, 666); err != nil {
		panic(err)
	}

	//Подготовим каналы и балансировщик
	links := make(chan string)
	quit := make(chan bool)
	b := new(Balancer)
	b.init(links)

	//Приготовимся перехватывать сигнал останова в канал keys
	keys := make(chan os.Signal, 1)
	signal.Notify(keys, os.Interrupt)

	//Запускаем балансировщик и генератор
	go b.balance(quit)
	go generator(links, ATATASTREAM, ATATAPOS)

	fmt.Println("Начинаем загрузку изображений")
	//Основной цикл программы:
	for {
		select {
		case <-keys: //пришла информация от нотификатора сигналов:
			fmt.Println("CTRL-C: Ожидаю завершения активных загрузок")
			quit <- true //посылаем сигнал останова балансировщику

		case <-quit: //пришло подтверждение о завершении от балансировщика
			fmt.Println("Загрузки завершены!")
			return
		}
	}
}
