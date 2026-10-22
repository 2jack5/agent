// AES-256-GCM 是业界最主流的对称加密算法
// AES	一种加密算法（Advanced Encryption Standard）
// 256	密钥长度 256 位（32 字节），非常强
// GCM	一种加密模式，自带防篡改能力
package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
)

// Encryptor AES-GCM 加解密器。
type Encryptor struct {
	gcm cipher.AEAD
}

// NewEncryptor 用主密钥派生 AES 密钥（sha256）。
func NewEncryptor(masterKey string) (*Encryptor, error) {
	if masterKey == "" {
		return nil, fmt.Errorf("secret_key is empty")
	}
	key := sha256.Sum256([]byte(masterKey))
	// 创建 AES 加密块
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("new cipher: %w", err)
	}
	// 套上 GCM 模式
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}
	return &Encryptor{gcm: gcm}, nil
}

// Encrypt 加密 → base64(nonce+ciphertext)。
func (e *Encryptor) Encrypt(plain string) (string, error) {
	if plain == "" {
		return "", nil
	}
	// 生成 nonce（重点）一次性随机数
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("read nonce: %w", err)
	}
	// 真正加密
	ct := e.gcm.Seal(nonce, nonce, []byte(plain), nil)
	return base64.StdEncoding.EncodeToString(ct), nil
}

// Decrypt 解密 base64(nonce+ciphertext)。
func (e *Encryptor) Decrypt(encoded string) (string, error) {
	if encoded == "" {
		return "", nil
	}
	// Base64 解码
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}
	if len(raw) < e.gcm.NonceSize() {
		return "", fmt.Errorf("ciphertext too short")
	}
	// 解密
	plain, err := e.gcm.Open(nil, raw[:e.gcm.NonceSize()], raw[e.gcm.NonceSize():], nil)
	if err != nil {
		return "", fmt.Errorf("gcm open: %w", err)
	}
	return string(plain), nil
}
