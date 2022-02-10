/*
 *    Copyright 2022 Whilein
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package w.redis;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * @author whilein
 */
public interface RedisBuilder {

    @NotNull RedisBuilder auth(@NotNull String username, @NotNull String password);

    @NotNull RedisBuilder auth(@NotNull String password);

    /**
     * Сменить {@link AsciiWriter}.
     * <p>
     * По умолчанию используется {@link AsciiWriter#defaultAsciiWriter()}.
     * <p>
     * Смотрите описание {@link AsciiWriter}, зачем это нужно.
     *
     * @param asciiWriter новый {@link AsciiWriter}
     * @return {@code this}
     */
    @NotNull RedisBuilder asciiWriter(@NotNull AsciiWriter asciiWriter);

    /**
     * Сменить изначальный размер буфера записи
     * <p>
     * По умолчанию изначальный размер равен {@code 1024}.
     *
     * @param capacity новый изначальный размер буфера записи
     * @return {@code this}
     */
    @NotNull RedisBuilder writeBufferCapacity(int capacity);

    /**
     * Сменить изначальный размер буфера чтения.
     * <p>
     * По умолчанию изначальный размер равен {@code 1024}.
     *
     * @param capacity новый изначальный размер буфера чтения
     * @return {@code this}
     */
    @NotNull RedisBuilder readBufferCapacity(int capacity);

    /**
     * Изменить таймаут подключения.
     * <p>
     * По умолчанию таймаут равен {@code 0}, т.е. ожидание подключения будет
     * вечным, следуя документации {@link java.nio.channels.Selector#select(long)}.
     *
     * @param timeout  таймаут
     * @param timeUnit единица времени, в которой измеряется таймаут
     * @return {@code this}
     */
    @NotNull RedisBuilder connectTimeout(long timeout, @NotNull TimeUnit timeUnit);

    /**
     * Изменить опцию {@code TCP_NODELAY} для канала.
     * <p>
     * По умолчанию значение равно {@code false}.
     *
     * @param tcpNoDelay новое значение опции {@code TCP_NODELAY}
     * @return {@code this}
     */
    @NotNull RedisBuilder tcpNoDelay(boolean tcpNoDelay);

    /**
     * Подключиться к {@code Redis} серверу.
     * <p>
     * Авторизация происходит только в том случае, если один из
     * методов {@link #auth(String)} или {@link #auth(String, String)}
     * был вызван.
     *
     * @return Объект, который позволяет отправлять на {@code Redis} сервер.
     * @throws RedisSocketException если не удалось подключиться
     * @throws RedisAuthException   если не удалось авторизоваться.
     */
    @NotNull Redis connect() throws RedisSocketException, RedisAuthException;

}
