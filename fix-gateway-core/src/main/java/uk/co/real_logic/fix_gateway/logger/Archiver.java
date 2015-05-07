/*
 * Copyright 2015 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway.logger;

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

public class Archiver implements Agent, DataHandler
{
    private static final int FRAGMENT_LIMIT = 10;

    private final IntFunction<StreamArchive> newStreamArchive = StreamArchive::new;
    private final Int2ObjectHashMap<StreamArchive> streamIdToArchive = new Int2ObjectHashMap<>();

    private final BufferFactory bufferFactory;
    private final Subscription subscription;

    public Archiver(final BufferFactory bufferFactory, final Subscription subscription)
    {
        this.bufferFactory = bufferFactory;
        this.subscription = subscription;
    }

    public void onData(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        streamIdToArchive.computeIfAbsent(header.streamId(), newStreamArchive)
                         .archive(buffer, offset, length, header);
    }

    private class StreamArchive
    {

        private final MutableDirectBuffer currentBuffer = new UnsafeBuffer(0, 0);
        private final int streamId;

        private ByteBuffer currentMappedBuffer;

        private int currentTermId = -1;

        public StreamArchive(final int streamId)
        {
            this.streamId = streamId;
        }

        public void archive(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            final int termId = header.termId();
            if (termId != currentTermId)
            {
                currentMappedBuffer = bufferFactory.map(LogDirectoryDescriptor.logFile(streamId, termId));
                currentBuffer.wrap(currentMappedBuffer);
                currentTermId = termId;
            }

            currentBuffer.putBytes(offset, buffer, offset, length);
        }

    }

    public int doWork() throws Exception
    {
        return subscription.poll(FRAGMENT_LIMIT);
    }

    public String roleName()
    {
        return "Archiver";
    }

    public void onClose()
    {
        subscription.close();
    }
}
