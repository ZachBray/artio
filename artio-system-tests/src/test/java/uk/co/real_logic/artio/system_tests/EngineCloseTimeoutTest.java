package uk.co.real_logic.artio.system_tests;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.unusedPort;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.ACCEPTOR_ID;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.INITIATOR_ID;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.INITIATOR_ID2;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.acceptingConfig;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.acceptingLibraryConfig;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.acquireSession;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.awaitLibraryConnect;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.connect;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.LangUtil;
import org.junit.Test;

import io.aeron.archive.ArchivingMediaDriver;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

public class EngineCloseTimeoutTest {
    private static final int LONG_TIMEOUT_IN_MS = 1_000_000_000;

    private FixEngine engine;
    private FixLibrary library;

    private final FakeOtfAcceptor otfAcceptor = new FakeOtfAcceptor();
    private final FakeHandler sessionHandler = new FakeHandler(otfAcceptor);
    private final TestSystem testSystem = new TestSystem();

    private int launchEngine()
    {
        final int port = unusedPort();
        final EngineConfiguration config = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        config.deleteLogFileDirOnStart(true);
        config.replyTimeoutInMs(LONG_TIMEOUT_IN_MS);
        config.initialAcceptedSessionOwner(InitialAcceptedSessionOwner.SOLE_LIBRARY);
        engine = FixEngine.launch(config);
        return port;
    }

    private FixLibrary connectLibrary()
    {
        final LibraryConfiguration config = acceptingLibraryConfig(sessionHandler);
        config.replyTimeoutInMs(LONG_TIMEOUT_IN_MS);

        return testSystem.add(connect(config));
    }

    @Test(timeout = 10_000)
    public void shouldNotTakeFullReplayTimeoutToCloseEngine() throws InterruptedException, IOException
    {
        try (final ArchivingMediaDriver mediaDriver = launchMediaDriver()) {
            System.out.println("MD launched");

            final int port = launchEngine();
            System.out.println("Engine launched");

            library = connectLibrary();
            awaitLibraryConnect(library);
            System.out.println("Library connected");

            try (FixConnection connection = FixConnection.initiate(port)) {
                logon(connection);

                assertEventuallyTrue(
                  "fix session connects",
                  () -> {
                      testSystem.poll();
                      return sessionHandler.sessions().size() > 0;
                  }
                );

                System.out.println("Session connected");

                final AtomicBoolean gate = new AtomicBoolean();
                new Thread(() -> {
                    engine.close();
                    gate.set(true);
                    System.out.println("Engine closed");
                }).start();

                while (!gate.get()) {
                    library.poll(10);
                    Thread.yield();
                }

                testSystem.close(library);
                System.out.println("Library closed");
            }
        }
    }

    void logon(final FixConnection connection)
    {
        connection.logon(true);
    }
}
