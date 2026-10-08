/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.gitea;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.client.GitCommit;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration tests against a running Gitea or Forgejo server configured with environment variables.
 * GITEA_IT_URL is the server URL, GITEA_IT_OWNER is the token owner, GITEA_IT_TOKEN has all scopes,
 * and GITEA_IT_READ_TOKEN has only the read:repository scope.
 */
@EnabledIfEnvironmentVariable(named = "GITEA_IT_URL", matches = ".+")
class GiteaRepositoryClientIT {

    private static final String BRANCH = "main";
    private static final String FLOW_PATH = "bucket/flow.json";
    private static final int VERSIONS = 121;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String url = System.getenv("GITEA_IT_URL");
    private final String owner = System.getenv("GITEA_IT_OWNER");
    private final String token = System.getenv("GITEA_IT_TOKEN");
    private final String readToken = System.getenv("GITEA_IT_READ_TOKEN");

    private StandardWebClientService webClientService;
    private WebClientServiceProvider webClientServiceProvider;
    private String repository;

    @BeforeEach
    void createRepository() throws Exception {
        webClientService = new StandardWebClientService();
        webClientServiceProvider = mock(WebClientServiceProvider.class);
        when(webClientServiceProvider.getWebClientService()).thenReturn(webClientService);
        when(webClientServiceProvider.getHttpUriBuilder()).thenAnswer(invocation -> new StandardHttpUriBuilder());

        repository = "it-" + UUID.randomUUID().toString().substring(0, 8);
        final byte[] body = MAPPER.writeValueAsBytes(Map.of("name", repository, "auto_init", true, "default_branch", BRANCH));
        try (HttpResponseEntity response = webClientService.post()
                .uri(java.net.URI.create(url + "/api/v1/user/repos"))
                .header("Authorization", "token " + token)
                .header("Content-Type", "application/json")
                .body(new ByteArrayInputStream(body), OptionalLong.of(body.length))
                .retrieve()) {
            assertEquals(201, response.statusCode(), new String(response.body().readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @AfterEach
    void closeClient() {
        webClientService.close();
    }

    @Test
    void testRegistryOperations() throws Exception {
        final GiteaRepositoryClient client = buildClient(token, "flows");
        assertTrue(client.hasReadPermission());
        assertTrue(client.hasWritePermission());

        assertTrue(client.getTopLevelDirectoryNames(BRANCH).isEmpty());
        final FlowRegistryException missingBranch = assertThrows(FlowRegistryException.class, () -> client.getTopLevelDirectoryNames("missing"));
        assertTrue(missingBranch.getMessage().contains("Branch [missing] not found"), missingBranch.getMessage());
        assertTrue(client.getContentSha(FLOW_PATH, BRANCH).isEmpty());
        assertTrue(client.getCommits(FLOW_PATH, BRANCH).isEmpty());

        final String firstCommit = client.createContent(request("version-0", null, null));
        assertEquals(1, client.getCommits(FLOW_PATH, BRANCH).size());
        assertEquals(java.util.Set.of("bucket"), client.getTopLevelDirectoryNames(BRANCH));
        assertEquals(java.util.Set.of("flow.json"), client.getFileNames("bucket", BRANCH));

        String lastCommit = firstCommit;
        for (int version = 1; version < VERSIONS; version++) {
            final String blobSha = client.getContentShaAtCommit(FLOW_PATH, lastCommit).orElseThrow();
            lastCommit = client.createContent(request("version-" + version, blobSha, lastCommit));
        }

        final List<GitCommit> commits = client.getCommits(FLOW_PATH, BRANCH);
        assertEquals(GiteaRepositoryClient.COMMIT_PAGE_SIZE, commits.size());
        assertEquals(lastCommit, commits.getFirst().id());
        assertEquals("Saving version-" + (VERSIONS - 1), commits.getFirst().message());

        try (InputStream content = client.getContentFromCommit(FLOW_PATH, firstCommit)) {
            assertEquals("version-0", new String(content.readAllBytes(), StandardCharsets.UTF_8));
        }
        try (InputStream content = client.getContentFromBranch(FLOW_PATH, BRANCH)) {
            assertEquals("version-" + (VERSIONS - 1), new String(content.readAllBytes(), StandardCharsets.UTF_8));
        }

        // Stale blob SHA must be rejected by the server
        final String staleBlobSha = client.getContentShaAtCommit(FLOW_PATH, firstCommit).orElseThrow();
        final FlowRegistryException conflict = assertThrows(FlowRegistryException.class,
                () -> client.createContent(request("conflict", staleBlobSha, firstCommit)));
        assertTrue(conflict.getMessage().contains("modified by another commit"), conflict.getMessage());

        // Application User author identity
        final String identity = "CN=admin, OU=NiFi";
        final String blobSha = client.getContentSha(FLOW_PATH, BRANCH).orElseThrow();
        client.createContent(GitCreateContentRequest.builder()
                .branch(BRANCH).path(FLOW_PATH).content("authored").message("Authored")
                .existingContentSha(blobSha).authorName(identity).authorEmail(identity).build());
        assertEquals(identity, client.getCommits(FLOW_PATH, BRANCH).getFirst().author());

        client.createBranch("feature", BRANCH, Optional.of(firstCommit));
        // Gitea 1.21 updates the branch listing asynchronously
        for (int attempt = 0; attempt < 20 && !client.getBranches().contains("feature"); attempt++) {
            Thread.sleep(500);
        }
        assertTrue(client.getBranches().containsAll(List.of(BRANCH, "feature")));
        try (InputStream content = client.getContentFromBranch(FLOW_PATH, "feature")) {
            assertEquals("version-0", new String(content.readAllBytes(), StandardCharsets.UTF_8));
        }
        assertThrows(FlowRegistryException.class, () -> client.createBranch("feature", BRANCH, Optional.empty()));

        try (InputStream deleted = client.deleteContent(FLOW_PATH, "Deregistering", BRANCH, identity, identity)) {
            assertEquals("authored", new String(deleted.readAllBytes(), StandardCharsets.UTF_8));
        }
        assertTrue(client.getContentSha(FLOW_PATH, BRANCH).isEmpty());
    }

    @Test
    void testSpecialCharacterPaths() throws Exception {
        final GiteaRepositoryClient client = buildClient(token, null);
        final String path = "bucket a %d/flow.json";
        client.createContent(GitCreateContentRequest.builder()
                .branch(BRANCH).path(path).content("special").message("Special").build());

        assertTrue(client.getTopLevelDirectoryNames(BRANCH).contains("bucket a %d"));
        assertEquals(java.util.Set.of("flow.json"), client.getFileNames("bucket a %d", BRANCH));
        assertEquals(1, client.getCommits(path, BRANCH).size());
        try (InputStream content = client.getContentFromBranch(path, BRANCH)) {
            assertEquals("special", new String(content.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void testReadOnlyToken() throws Exception {
        final GiteaRepositoryClient client = buildClient(readToken, null);
        assertTrue(client.hasReadPermission());

        final FlowRegistryException exception = assertThrows(FlowRegistryException.class, () ->
                client.createContent(GitCreateContentRequest.builder()
                        .branch(BRANCH).path(FLOW_PATH).content("denied").message("Denied").build()));
        assertTrue(exception.getMessage().contains("write:repository"), exception.getMessage());
        assertFalse(client.getBranches().isEmpty());
    }

    private GiteaRepositoryClient buildClient(final String accessToken, final String repositoryPath) throws FlowRegistryException {
        return GiteaRepositoryClient.builder()
                .clientId("it")
                .apiUrl(url)
                .repoOwner(owner)
                .repoName(repository)
                .repoPath(repositoryPath)
                .accessToken(accessToken)
                .webClient(webClientServiceProvider)
                .logger(mock(ComponentLog.class))
                .build();
    }

    private GitCreateContentRequest request(final String content, final String blobSha, final String expectedCommit) {
        return GitCreateContentRequest.builder()
                .branch(BRANCH)
                .path(FLOW_PATH)
                .content(content)
                .message("Saving " + content)
                .existingContentSha(blobSha)
                .expectedCommitSha(expectedCommit)
                .build();
    }
}
