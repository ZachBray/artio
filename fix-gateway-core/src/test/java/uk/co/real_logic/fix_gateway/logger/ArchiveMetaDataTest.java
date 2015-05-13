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

import org.junit.Test;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataDecoder;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class ArchiveMetaDataTest
{
    public static final int STREAM_ID = 1;
    public static final int INITIAL_TERM_ID = 12;
    public static final int TERM_BUFFER_LENGTH = 13;

    private ByteBuffer buffer = ByteBuffer.allocate(8 * 1024);
    private ExistingBufferFactory existingBufferFactory = file -> buffer;
    private BufferFactory newBufferFactory = (file, size) -> buffer;
    private ArchiveMetaData archiveMetaData = new ArchiveMetaData(existingBufferFactory, newBufferFactory);

    @Test
    public void shouldStoreMetaDataInformation()
    {
        archiveMetaData.write(STREAM_ID, INITIAL_TERM_ID, TERM_BUFFER_LENGTH);

        final ArchiveMetaDataDecoder decoder = archiveMetaData.read(STREAM_ID);
        assertEquals(INITIAL_TERM_ID, decoder.initialTermId());
        assertEquals(TERM_BUFFER_LENGTH, decoder.termBufferLength());
    }

}
