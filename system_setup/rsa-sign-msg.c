
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <string.h>

// Helper to decode Base64 using OpenSSL BIOs
unsigned char* base64_decode(const char* input, int length, int* out_len) {
    BIO *b64, *bmem;
    unsigned char* buffer = (unsigned char*)malloc(length);
    memset(buffer, 0, length);

    b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL); // Handle single-line b64
    bmem = BIO_new_mem_buf(input, length);
    bmem = BIO_push(b64, bmem);

    *out_len = BIO_read(bmem, buffer, length);
    BIO_free_all(bmem);
    return buffer;
}

int verify_signature_b64key(const char* public_key_b64, const char* message, const char* signature_b64) {
    int success = 0;
    int der_len, sig_len;

    // 1. Decode Base64 strings
    unsigned char* der_data = base64_decode(public_key_b64, strlen(public_key_b64), &der_len);
    unsigned char* sig_data = base64_decode(signature_b64, strlen(signature_b64), &sig_len);

    // 2. Load Public Key from DER data
    const unsigned char* p = der_data;
    EVP_PKEY* pub_key = d2i_PUBKEY(NULL, &p, der_len);

    if (pub_key) {
        EVP_MD_CTX* ctx = EVP_MD_CTX_new();

        // 3. Initialize Verification (Default is PKCS1 v1.5 padding)
        if (EVP_DigestVerifyInit(ctx, NULL, EVP_sha256(), NULL, pub_key) == 1) {
            EVP_DigestVerifyUpdate(ctx, message, strlen(message));

            // 4. Finalize and verify
            if (EVP_DigestVerifyFinal(ctx, sig_data, sig_len) == 1) {
                success = 1; // Verified!
            }
        }
        EVP_MD_CTX_free(ctx);
        EVP_PKEY_free(pub_key);
    }

    free(der_data);
    free(sig_data);
    return success;
}

// Helper function to load a public key from a PEM file
EVP_PKEY* load_public_key(const char* public_key_path) {
    FILE* fp = fopen(public_key_path, "r");
    if (fp == NULL) {
        fprintf(stderr, "Error loading public key file %s\n", public_key_path);
        return NULL;
    }
    EVP_PKEY* pubKey = PEM_read_PUBKEY(fp, NULL, NULL, NULL);
    fclose(fp);
    if (pubKey == NULL) {
        fprintf(stderr, "Error reading public key from file\n");
    }
    return pubKey;
}


int main(int argc, char *argv[]) {
    //char fname[32];
    // Load public key (replace with your key path)
    EVP_PKEY* publicKey = load_public_key("public_key.pem");

    if (publicKey == NULL) {
        return 1;
    }
    if(argc < 2) 
    { 
	    printf("Usage: ./a.out msg-sig-file.json\n");
	    return -1;
    }
    //scanf(fname, "%s", argv[2]);
    printf("read fname=%s\n", argv[1]);
    // Example message and a dummy signature (replace with actual data)
    unsigned char* message = (unsigned char*)"Message for RSA signing";
    unsigned char* msg1 = (unsigned char*)"msg1";
    const char* msg2 = "SELECT * FROM usertable WHERE YCSB_KEY=868;";
    size_t message_len = strlen((char*)msg2);
    // Signature needs to be a valid signature for the message/key combo
    // The length will match the RSA key size (e.g., 256 bytes for a 2048-bit key)
    unsigned char signature[256] = { /* ... your signature bytes ... */ };
    unsigned char signature_b64[177] = "VhQi/saMAtkgTICK573XFJqvE8yot/ZnZ41iu/odgyN4lEC/ZY3IObSxANiuzAc6euRpjzAvu/uYbw9whff9GUlAfJu8SKTaBTbcvn12ZYKlHSPUlt9DZyWGuVlH8LU3hfDMT2Ao3+XyrkqwyE5NqzAaTs0MPYREiZLSfqmpJmsBJIE=";
    const char* queryString = "SELECT * FROM usertable WHERE YCSB_KEY=868;|ggwX8rWewlbGl4EwBnf1Xb/KW+ReZ/e9r5tDGeysRDihIDih1DRNtAWpDh5Zf2LFEEY7IlKM9U9YmeNLgMbUKwKtatjxU3e3/ekBJ0fzhUg1vVagqmauVRmPzbM+G2WaWInrD/pK4VIlgQSN87+po2lCLdMgttSqI7e9w5bA49OAKFo=";
    const char *query_string2 ="00000000 Tc9AUiabR+rDdurgAsCajKnH6xwNDIZX+aJ/4YhFnmHBx8i05p5XxtKBbLdTY73VcIqeprryc3v1vCs18Yqp89LGWKMVmrvw1ItQURlfUv7XCjm9kZxGyM1T1XEOWnp9GsUaU/G3WlkGVrc3oL5jVUop+re4681NB8eo7uitHm7No7M=::SELECT * FROM usertable WHERE YCSB_KEY=831;";
    const char *signature2 = "ggwX8rWewlbGl4EwBnf1Xb/KW+ReZ/e9r5tDGeysRDihIDih1DRNtAWpDh5Zf2LFEEY7IlKM9U9YmeNLgMbUKwKtatjxU3e3/ekBJ0fzhUg1vVagqmauVRmPzbM+G2WaWInrD/pK4VIlgQSN87+po2lCLdMgttSqI7e9w5bA49OAKFo=";
    size_t signature_len = sizeof(signature);
    size_t signature_len64 = sizeof(signature_b64);
    size_t signature_len2 = sizeof(signature2);
    const char* publicKey2 = "MIGiMA0GCSqGSIb3DQEBAQUAA4GQADCBjAKBhACFuG2SZFx6fduyYp8aQ5p6TjCKaytg2mM9afk6fmAwKs78IkGbXAIpS5YDYr8OilU8w7kfBJXjqLvlEnLh2sjAkmZEMJHzL8dkUZfzp0L6SwCsszXmzOk2uraVcnzeInP9xMuEScZ+Ss90vWQa/67c1UX0X0eIGyTONGZ5SkXNWu+bWwIDAQAB";

    // Perform verification
    //verify_signature(message, message_len, signature, signature_len, publicKey);
    //verify_signature(msg1, message_len, signature_b64, signature_len64, publicKey);
    int valid = -1;
    valid = verify_signature_b64key( publicKey2, msg2, signature2); 
    printf("msg %s sig valid: %d\n", msg2, valid);
    valid = verify_signature_b64key( publicKey2, "abracadabra", signature2); 
    printf("msg abracadabra sig valid: %d\n", valid);

     int i = 0;
     int sign = 0;
     for(i=0; query_string2[i] != ':';i++) ;
     if(query_string2[i+1] == ':')
           sign = 1;
     char qmsg[1024];
     char qsig[1024] = {};
     strcpy(qmsg, (const char *) (&query_string2[i+2]));
     printf("msg %s sig: %s\n", qmsg, qsig);
     strncpy(qsig, (const char *) (query_string2+9), i-9);
     printf(" sig: %s\n", qsig);
    valid = verify_signature_b64key( publicKey2, qmsg, qsig); 
    printf("qmsg %s qsig valid: %d\n", qmsg, valid);

    printf("qmsg qsig test: \n");
    char *signTx = "HfncuJiCpRGdJQyPEPtA+kWPX8DYuTJ5Ec9MIJ5P9uhklyHhi66KFVe8g3tmgne8E1Hc51qgffRGpNkjps7j8DJtORDCNA8my8wYZ4fOZKB4cwuu+JXfSAV3XfXs6nyotcsGRUchI/umA4X7T1yvuzeKDeUl2B+XkHNSs7Y+j4camuQ=##SELECT * FROM usertable where YCSB_KEY=473;";
     for(i=0; signTx[i] != '#';i++) ;
     strcpy(qmsg, (const char *) (&signTx[i+2]));
     strncpy(qsig, (const char *) signTx, i);
     printf("i = %d msg %s sig: %s\n", i, qmsg, qsig);
    valid = verify_signature_b64key( publicKey2, qmsg, qsig); 
    printf("qmsg %s qsig valid: %d\n", qmsg, valid);


    // Clean up
    EVP_PKEY_free(publicKey);

    return 0;
}

