package main

import (
	"log"

	"github.com/nats-io/jwt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func main() {
	// 假设这些是你的用户密钥对和操作者的公钥
	userPublicKey := "UCD67V6N5IE2BSHNJXLZ33U7TLKFNCUUU7KO2PQQRCIUKCHXEVY42DV7"
	userSeed := "SAACLS5X4ZFNEUJPHADLXUG6I2NXNDGYA6ZZJ2C244UZQUV2D7Z2CFHNG4"

	// 加载用户的nkeys
	ukp, err := nkeys.FromSeed([]byte(userSeed))
	if err != nil {
		log.Fatal("Failed to create nkeys from seed:", err)
	}

	// 创建用户JWT
	claims := jwt.NewUserClaims(userPublicKey)
	claims.Name = "test-user"
	encodedJWT, err := claims.Encode(ukp)
	if err != nil {
		log.Fatal("Failed to encode JWT:", err)
	}

	// 连接到NATS服务器
	opts := []nats.Option{
		nats.Name("NATS Sample Client"),
		nats.Token(encodedJWT),
	}

	// 连接到NATS
	nc, err := nats.Connect(nats.DefaultURL, opts...)
	if err != nil {
		log.Fatal("Error connecting to NATS:", err)
	}
	defer nc.Close()

	log.Println("Connected to NATS successfully!")
	// 这里可以继续进行消息的订阅或发布等操作

	// 订阅"test-master"主题并设置消息处理函数
	_, err = nc.Subscribe("test-master", func(m *nats.Msg) {
		log.Printf("Received message on [%s]: %s", m.Subject, string(m.Data))
	})
	if err != nil {
		log.Fatal("Error subscribing to topic:", err)
	}

	// 防止主程序退出
	select {}
}
