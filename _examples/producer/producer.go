// This example declares a durable exchange, and publishes one messages to that
// exchange. This example allows up to 8 outstanding publisher confirmations
// before blocking publishing.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("exchange", "test-exchange", "Durable AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	queue        = flag.String("queue", "test-queue", "Ephemeral AMQP queue name")
	routingKey   = flag.String("key", "test-key", "AMQP routing key")
	body         = flag.String("body", "foobar", "Body of message")
	continuous   = flag.Bool("continuous", false, "Keep publishing messages at a 1msg/sec rate")
	WarnLog      = log.New(os.Stderr, "[WARNING] ", log.LstdFlags|log.Lmsgprefix)
	ErrLog       = log.New(os.Stderr, "[ERROR] ", log.LstdFlags|log.Lmsgprefix)
	Log          = log.New(os.Stdout, "[INFO] ", log.LstdFlags|log.Lmsgprefix)
)

func init() {
	flag.Parse()
}

func main() {
	exitCh := make(chan struct{})                       // 退出通道
	confirmsCh := make(chan *amqp.DeferredConfirmation) // 确认管道
	confirmsDoneCh := make(chan struct{})               // 确认完成管道
	// Note: this is a buffered channel so that indicating OK to
	// publish does not block the confirm handler
	publishOkCh := make(chan struct{}, 1) // 生产完成管道
	// 设置关闭处理器
	setupCloseHandler(exitCh)
	// 启动确认处理器
	startConfirmHandler(publishOkCh, confirmsCh, confirmsDoneCh, exitCh)
	// 发布消息
	publish(publishOkCh, confirmsCh, confirmsDoneCh, exitCh)
}

func setupCloseHandler(exitCh chan struct{}) {
	// 操作系统信号是一种通知机制，用于告知进程发生了某些事件。
	// 例如，当用户在终端中按下Ctrl + C（对应os.Interrupt信号）或者
	// 系统发送SIGTERM信号（通常用于优雅地终止进程）时，操作系统会向相关进程发送这些信号。
	// 如果没有缓冲通道，当进程正在处理一个信号时，下一个信号到来时可能会被忽略，而有了缓冲通道，可以在一定程度上避免这种情况的发生
	c := make(chan os.Signal, 2) // 信号管道，用于监听是否手动结束，“Ctrl+C”
	// 通过signal.Notify(c, os.Interrupt, syscall.SIGTERM)，
	// 将c通道注册为接收os.Interrupt和syscall.SIGTERM这两种信号的通道。
	// 这意味着当进程接收到这两种信号中的任何一种时，信号将会被发送到c通道中。
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c // 监听阻塞
		Log.Printf("close handler: Ctrl+C pressed in Terminal")
		close(exitCh) // 关闭存在通道
	}()
}

// publish 生产者队列写入消息
func publish(publishOkCh <-chan struct{}, confirmsCh chan<- *amqp.DeferredConfirmation, confirmsDoneCh <-chan struct{}, exitCh chan struct{}) {
	// DialConfig和Open中使用Config来指定在连接打开握手过程中使用的所需调优参数。
	// 商定的调优将存储在返回的连接的Config字段中。
	config := amqp.Config{
		Vhost:      "/",
		Properties: amqp.NewConnectionProperties(), // 性质
	}
	config.Properties.SetClientConnectionName("producer-with-confirms")

	// 代理连接配置，新建连接
	Log.Printf("producer: dialing %s", *uri)
	conn, err := amqp.DialConfig(*uri, config)
	if err != nil {
		ErrLog.Fatalf("producer: error in dial: %s", err)
	}
	defer conn.Close()

	// 获取管道
	Log.Println("producer: got Connection, getting Channel")
	channel, err := conn.Channel()
	if err != nil {
		ErrLog.Fatalf("error getting a channel: %s", err)
	}
	defer channel.Close()

	Log.Printf("producer: declaring exchange")
	if err := channel.ExchangeDeclare( // 申请交换器
		*exchange,     // name
		*exchangeType, // type
		true,          // durable
		false,         // auto-delete
		false,         // internal
		false,         // noWait
		nil,           // arguments
	); err != nil {
		ErrLog.Fatalf("producer: Exchange Declare: %s", err)
	}

	Log.Printf("producer: declaring queue '%s'", *queue)
	queue, err := channel.QueueDeclare( // 申请队列
		*queue, // name of the queue
		true,   // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // noWait
		nil,    // arguments
	)
	if err == nil { // 得到队列
		Log.Printf("producer: declared queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
			queue.Name, queue.Messages, queue.Consumers, *routingKey)
	} else {
		ErrLog.Fatalf("producer: Queue Declare: %s", err)
	}

	Log.Printf("producer: declaring binding")
	// 将队列和路由密钥和交换器绑定在一起
	if err := channel.QueueBind(queue.Name, *routingKey, *exchange, false, nil); err != nil {
		ErrLog.Fatalf("producer: Queue Bind: %s", err)
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	Log.Printf("producer: enabling publisher confirms.")
	// 尝试将通道设置为确认模式
	if err := channel.Confirm(false); err != nil { // 发布确认，会等待确认消费成功
		ErrLog.Fatalf("producer: channel could not be put into confirm mode: %s", err)
	}

	// 无限循环，不断尝试发布消息，直到接收到停止信号。
	for {
		canPublish := false                                      // 初始化一个标志，表示是否可以发布消息。
		Log.Println("producer: waiting on the OK to publish...") // 正在等待发布许可
		// 判断是否能成功发布消息
		for {
			select {
			// 如果接收到确认完成通道的信号，表示所有确认都已处理完毕，程序退出。
			case <-confirmsDoneCh: // 生产者确认关闭
				Log.Println("producer: stopping, all confirms seen")
				return
			// 如果接收到发布许可通道的信号，表示可以发布消息。将canPublish设置为true，并跳出内部循环。
			case <-publishOkCh: // 生产者 确认能够成功发布
				Log.Println("producer: got the OK to publish")
				canPublish = true
				break
			// 如果在一秒钟内没有收到任何信号，打印警告日志表示仍在等待发布许可。
			case <-time.After(time.Second): // 还在等待 确认发布成功的 OK
				WarnLog.Println("producer: still waiting on the OK to publish...")
				continue
			}
			if canPublish {
				break
			}
		}
		// 如果canPublish为true，正在发布消息，并显示消息的大小和内容。
		Log.Printf("producer: publishing %dB body (%q)", len(*body), *body)
		// 使用带延迟确认的方式发布消息，并获取延迟确认对象。
		dConfirmation, err := channel.PublishWithDeferredConfirm( // 带延迟确认的方式
			*exchange,
			*routingKey,
			true,
			false,
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				DeliveryMode:    amqp.Persistent,
				Priority:        0,
				AppId:           "sequential-producer",
				Body:            []byte(*body),
			},
		)
		// 如果发布过程中出现错误，程序将以错误退出。
		if err != nil {
			ErrLog.Fatalf("producer: error in publish: %s", err)
		}

		// 使用select语句处理发布后的情况
		// 将发布的消息写入待确认的消息队列
		// 当这些消息被消费消费被处理之后，就可以从待确认队列中拿出，然后等confirmsCh队列中的所有的消息都确认完毕之后，消费者就会往confirmsDoneCh队列中传入一个确认信号，表示所有待确认的消息都已经处理完了
		select {
		case <-confirmsDoneCh: // 如果接收到确认完成通道的信号，表示所有确认都已处理完毕，程序退出。
			Log.Println("producer: stopping, all confirms seen")
			return
		// 生产者在成功发布消息并获取到延迟确认对象（dConfirmation）后，会将这个延迟确认对象发送到confirmsCh通道。
		// 这个通道主要是生产者和确认处理器（confirm handler）之间传递消息确认相关对象的管道。
		// 对于放入confirmsCh中的消息（这里确切说是延迟确认对象），确认处理器会从这个通道接收这些对象，
		// 并根据这些对象来检查消息是否被成功消费（通过检查Acked()等类似的方法）。
		// 也就是说，放入confirmsCh中的对象是用于确认消息是否被正确消费的依据。
		case confirmsCh <- dConfirmation: // 将发布的消息 写入 待确认消息队列， 已交付，已延迟确认给处理员
			Log.Println("producer: delivered deferred confirm to handler")
			break
		}

		// 等待确认或停止
		select {
		case <-confirmsDoneCh: // 如果接收到确认完成通道的信号，表示所有确认都已处理完毕，程序退出。
			Log.Println("producer: stopping, all confirms seen")
			return
		case <-time.After(time.Millisecond * 250): // 如果在 250 毫秒内没有收到确认完成信号
			if *continuous { // 持续发布模式，继续下一次循环
				continue
			} else { // 表示非持续发布模式
				Log.Println("producer: initiating stop")
				close(exitCh) // 关闭退出通道，向其他部分发送退出信号
				select {
				case <-confirmsDoneCh:
					Log.Println("producer: stopping, all confirms seen")
					return
				case <-time.After(time.Second * 10):
					// 如果在 10 秒钟内没有收到确认完成信号，打印警告日志表示可能在未处理完所有确认的情况下停止。
					WarnLog.Println("producer: may be stopping with outstanding confirmations")
					return
				}
			}
		}
	}
}

// startConfirmHandler 键值对存储未处理的确认信息
func startConfirmHandler(publishOkCh chan<- struct{}, confirmsCh <-chan *amqp.DeferredConfirmation, confirmsDoneCh chan struct{}, exitCh <-chan struct{}) {
	go func() {
		// 创建了一个映射，用于存储未处理的确认信息。键是消息的投递标签（DeliveryTag），值是对应的确认信息对象
		confirms := make(map[uint64]*amqp.DeferredConfirmation)
		// 启动了一个无限循环，不断地处理确认信息和控制消息发布流程。
		for {
			// 在循环中，首先检查是否接收到了退出信号。
			select {
			case <-exitCh: // 如果接收到退出信号，调用exitConfirmHandler函数进行清理工作，并返回，退出确认处理器
				exitConfirmHandler(confirms, confirmsDoneCh)
				return
			default:
				break
			}
			// 计算了未处理的确认信息的数量
			outstandingConfirmationCount := len(confirms)

			// Note: 8 is arbitrary, you may wish to allow more outstanding confirms before blocking publish
			if outstandingConfirmationCount <= 8 {
				// 如果未确认消息数量小于等于 8，则尝试向publishOkCh通道发送一个信号，表示可以进行消息发布。
				select {
				case publishOkCh <- struct{}{}: // 这可能会触发生产者进行下一次消息发布
					Log.Println("confirm handler: sent OK to publish")
				case <-time.After(time.Second * 5): // 如果在 5 秒内没有成功发送信号（可能是由于通道已满或其他原因），
					// 则会打印一条警告信息。
					WarnLog.Println("confirm handler: timeout indicating OK to publish (this should never happen!)")
				}
			} else { // 如果未确认消息数量大于 8，则打印一条警告信息，表示正在等待未确认消息的处理，阻止消息发布
				WarnLog.Printf("confirm handler: waiting on %d outstanding confirmations, blocking publish", outstandingConfirmationCount)
			}

			select {
			case confirmation := <-confirmsCh: // 从confirmsCh通道接收确认信息，
				dtag := confirmation.DeliveryTag
				// 并将其存储在confirms映射中，使用消息的投递标签作为键。
				confirms[dtag] = confirmation
			case <-exitCh: // 检查是否接收到了退出信号。
				exitConfirmHandler(confirms, confirmsDoneCh)
				return
			}
			checkConfirmations(confirms)
		}
	}()
}

func exitConfirmHandler(confirms map[uint64]*amqp.DeferredConfirmation, confirmsDoneCh chan struct{}) {
	Log.Println("confirm handler: exit requested")
	waitConfirmations(confirms)
	close(confirmsDoneCh)
	Log.Println("confirm handler: exiting")
}

func checkConfirmations(confirms map[uint64]*amqp.DeferredConfirmation) {
	Log.Printf("confirm handler: checking %d outstanding confirmations", len(confirms))
	for k, v := range confirms {
		if v.Acked() { // 检查当前确认信息对象是否已被确认（通过调用Acked()方法）
			Log.Printf("confirm handler: confirmed delivery with tag: %d", k)
			delete(confirms, k) // 从确认信息映射中删除已确认消息的条目，以保持映射中只包含未确认的消息。
		}
	}
}

func waitConfirmations(confirms map[uint64]*amqp.DeferredConfirmation) {
	Log.Printf("confirm handler: waiting on %d outstanding confirmations", len(confirms))

	checkConfirmations(confirms)

	for k, v := range confirms {
		select {
		case <-v.Done():
			Log.Printf("confirm handler: confirmed delivery with tag: %d", k)
			delete(confirms, k)
		case <-time.After(time.Second):
			WarnLog.Printf("confirm handler: did not receive confirmation for tag %d", k)
		}
	}
	// 剩下的未完成确认的信的数量
	outstandingConfirmationCount := len(confirms)
	if outstandingConfirmationCount > 0 {
		ErrLog.Printf("confirm handler: exiting with %d outstanding confirmations", outstandingConfirmationCount)
	} else {
		Log.Println("confirm handler: done waiting on outstanding confirmations")
	}
}
