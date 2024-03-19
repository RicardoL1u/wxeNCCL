package main

import (
	"fmt"
	"io/ioutil"
	"time"

	"reflect"

	"github.com/nats-io/jwt"
	"github.com/nats-io/nkeys"
)

func main() {
	akp, err := nkeys.CreateAccount()
	if err != nil {
		panic(err)
	}

	seed, err := akp.Seed()
	if err != nil {
		fmt.Println("获取密钥种子失败:", err)
		return
	}

	// 将密钥种子转换为字符串
	seedStr := string(seed)

	// 保存到文件
	err = ioutil.WriteFile("account_seed.txt", []byte(seedStr), 0644)
	if err != nil {
		fmt.Println("保存密钥到文件失败:", err)
		return
	}

	fmt.Println("账户密钥已保存到 account_seed.txt")
	accPublicKey, err := akp.PublicKey()
	if err != nil {
		panic(err)
	}
	fmt.Println("akp", akp, reflect.TypeOf(akp))

	// 创建账户的 JWT
	accJwt := jwt.NewAccountClaims(accPublicKey)
	accJwt.Name = "taskAccount"
	accJwt.Expires = time.Now().AddDate(1, 0, 0).Unix()

	// 设置账户的权限和导出
	accJwt.Limits.Conn = 10
	accJwt.Limits.Subs = 100
	// 创建用户的 Nkey 对
	ukp, err := nkeys.CreateUser()
	if err != nil {
		panic(err)
	}
	userPublicKey, err := ukp.PublicKey()
	if err != nil {
		panic(err)
	}

	// 创建用户的 JWT
	userJwt := jwt.NewUserClaims(userPublicKey)
	userJwt.Name = "user1"
	userJwt.Expires = time.Now().AddDate(1, 0, 0).Unix()

	// 设置用户的权限和订阅
	userJwt.Pub.Allow.Add("taskID.>")
	userJwt.Sub.Allow.Add("taskID.>")

	// 使用账户的私钥对用户 JWT 进行签名
	userJwtStr, err := userJwt.Encode(akp)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Account Public Key: %s\n", accPublicKey)

	//输出用户的公钥和 JWT
	fmt.Printf("User Public Key: %s\n", userPublicKey)
	fmt.Printf("User JWT: %s\n", userJwtStr)
}
