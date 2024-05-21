package com.github.simplenet.utility.exposed.cryptography;

import com.github.simplenet.Client;

import javax.crypto.Cipher;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

@FunctionalInterface
public interface CryptographicFunction {

    CryptographicFunction DO_FINAL = (Cipher cipher, ByteBuffer data) -> {
        ByteBuffer output = data.duplicate().limit(cipher.getOutputSize(data.limit()));
        cipher.doFinal(data, output);
        return output;
    };

    CryptographicFunction UPDATE = (Cipher cipher, ByteBuffer data) -> {
        ByteBuffer output = data.duplicate().limit(cipher.getOutputSize(data.limit()));
        cipher.update(data, output);
        return output;
    };

    ByteBuffer apply(Cipher cipher, ByteBuffer buffer) throws GeneralSecurityException;
   
}