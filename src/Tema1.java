import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Tema1 {
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: java Tema1 <num_threads> <articles_file> <auxiliary_file>");
            return;
        }
        
        int numThreads = Integer.parseInt(args[0]);
        String articlesFile = args[1];
        String auxiliaryFile = args[2];
        
        // Read auxiliary files
        BufferedReader auxReader = new BufferedReader(new FileReader(auxiliaryFile));
        int numAuxFiles = Integer.parseInt(auxReader.readLine().trim());
        String languagesFile = auxReader.readLine().trim();
        String categoriesFile = auxReader.readLine().trim();
        String linkingWordsFile = auxReader.readLine().trim();
        auxReader.close();
        
        // Resolve paths relative to auxiliary file location
        File auxParent = new File(auxiliaryFile).getParentFile();
        Set<String> languages = readListFile(new File(auxParent, languagesFile).getCanonicalPath());
        Set<String> categories = readListFile(new File(auxParent, categoriesFile).getCanonicalPath());
        Set<String> linkingWords = readListFile(new File(auxParent, linkingWordsFile).getCanonicalPath());
        
        // Read article files list
        BufferedReader articlesReader = new BufferedReader(new FileReader(articlesFile));
        int numFiles = Integer.parseInt(articlesReader.readLine().trim());
        List<String> resolvedFiles = new ArrayList<>();
        
        File articlesParent = new File(articlesFile).getParentFile();
        for (int i = 0; i < numFiles; i++) {
            String path = articlesReader.readLine().trim();
            File jsonFile = new File(articlesParent, path);
            resolvedFiles.add(jsonFile.getCanonicalPath());
        }
        articlesReader.close();
        
        // Process articles in parallel
        NewsAggregator aggregator = new NewsAggregator(numThreads, languages, categories, linkingWords);
        aggregator.processArticles(resolvedFiles);
        aggregator.generateOutputs();
    }
    
    private static Set<String> readListFile(String filename) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        int count = Integer.parseInt(br.readLine().trim());
        Set<String> result = new HashSet<>();
        for (int i = 0; i < count; i++) {
            result.add(br.readLine().trim());
        }
        br.close();
        return result;
    }
}

class Article {
    public String uuid;
    public String title;
    public String author;
    public String url;
    public String text;
    public String published;
    public String language;
    public List<String> categories;
    
    public Article(JsonNode node) {
        this.uuid = node.get("uuid").asText();
        this.title = node.get("title").asText();
        this.author = node.has("author") ? node.get("author").asText() : "";
        this.url = node.get("url").asText();
        this.text = node.has("text") ? node.get("text").asText() : "";
        this.published = node.get("published").asText();
        this.language = node.has("language") ? node.get("language").asText() : "";
        
        this.categories = new ArrayList<>();
        if (node.has("categories")) {
            JsonNode catNode = node.get("categories");
            if (catNode.isArray()) {
                for (JsonNode cat : catNode) {
                    this.categories.add(cat.asText());
                }
            }
        }
    }
}

class NewsAggregator {
    private final int numThreads;
    private final Set<String> validLanguages;
    private final Set<String> validCategories;
    private final Set<String> linkingWords;
    
    private final ConcurrentHashMap<String, Article> uuidToArticle = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Article> titleToArticle = new ConcurrentHashMap<>();
    private final Set<String> invalidUuids = ConcurrentHashMap.newKeySet();
    private final Set<String> invalidTitles = ConcurrentHashMap.newKeySet();
    private final AtomicInteger duplicatesCount = new AtomicInteger(0);
    
    private final List<Article> validArticles = Collections.synchronizedList(new ArrayList<>());
    private final ConcurrentHashMap<String, List<String>> categoryArticles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<String>> languageArticles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> authorCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> normalizedToReal = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> keywordsPerArticle = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> keywordsCount = new ConcurrentHashMap<>();
    
    private final ObjectMapper mapper = new ObjectMapper();
    
    public NewsAggregator(int numThreads, Set<String> languages, Set<String> categories, Set<String> linkingWords) {
        this.numThreads = numThreads;
        this.validLanguages = languages;
        this.validCategories = categories;
        this.linkingWords = linkingWords;
        
        // Initialize category maps with normalized names
        for (String cat : categories) {
            String norm = cat.replace(",", "").replaceAll("\\s+", "_");
            categoryArticles.put(norm, Collections.synchronizedList(new ArrayList<>()));
            normalizedToReal.put(norm, cat);
        }
        
        // Initialize language maps
        for (String lang : languages) {
            languageArticles.put(lang, Collections.synchronizedList(new ArrayList<>()));
        }
    }
    
    public void processArticles(List<String> files) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(files.size());
        
        for (String file : files) {
            executor.submit(() -> {
                try {
                    processFile(file);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        executor.shutdown();
    }
    
    private void processFile(String filename) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
        br.close();
        
        JsonNode root = mapper.readTree(sb.toString());
        
        if (root.isArray()) {
            for (JsonNode node : root) {
                processArticle(node);
            }
        }
    }
    
    private void processArticle(JsonNode node) {
        Article article = new Article(node);
        
        // If this uuid/title was already marked invalid, skip directly
        if (invalidUuids.contains(article.uuid) || invalidTitles.contains(article.title)) {
            duplicatesCount.incrementAndGet();
            return;
        }
        
        // 1) Duplicate UUID → remove existing article too
        Article prevByUuid = uuidToArticle.putIfAbsent(article.uuid, article);
        if (prevByUuid != null) {
            invalidUuids.add(article.uuid);
            uuidToArticle.remove(article.uuid);
            removeArticle(prevByUuid);
            duplicatesCount.addAndGet(2); // count both the old and current article
            return;
        }
        
        // 2) Duplicate title → remove existing article + mark entire title invalid
        Article prevByTitle = titleToArticle.putIfAbsent(article.title, article);
        if (prevByTitle != null) {
            invalidTitles.add(article.title);
            invalidUuids.add(article.uuid);
            removeArticle(prevByTitle);
            uuidToArticle.remove(article.uuid); // clean up the insertion made above
            titleToArticle.remove(article.title);
            duplicatesCount.addAndGet(2); // count both articles involved
            return;
        }
        
        // 3) Valid article: save in lists and mappings
        synchronized (validArticles) {
            validArticles.add(article);
        }
        
        if (article.author != null && !article.author.isEmpty()) {
            authorCounts.computeIfAbsent(article.author, k -> new AtomicInteger()).incrementAndGet();
        }
        
        // Categories (unique per article)
        if (article.categories != null) {
            Set<String> uniqueCats = new HashSet<>();
            for (String c : article.categories) {
                String norm = c.replace(",", "").replaceAll("\\s+", "_");
                if (!normalizedToReal.containsKey(norm)) {
                    continue; // non-validated category
                }
                uniqueCats.add(norm);
            }
            for (String norm : uniqueCats) {
                categoryArticles.computeIfAbsent(norm, k -> Collections.synchronizedList(new ArrayList<>())).add(article.uuid);
            }
        }
        
        // Languages (only valid ones)
        if (article.language != null && validLanguages.contains(article.language)) {
            languageArticles.computeIfAbsent(article.language, k -> Collections.synchronizedList(new ArrayList<>())).add(article.uuid);
        }
        
        // Keywords for English
        if ("english".equals(article.language) && article.text != null) {
            Set<String> keywords = extractKeywords(article.text);
            if (!keywords.isEmpty()) {
                keywordsPerArticle.put(article.uuid, keywords);
                for (String w : keywords) {
                    keywordsCount.computeIfAbsent(w, k -> new AtomicInteger()).incrementAndGet();
                }
            }
        }
    }
    
    private void removeArticle(Article a) {
        if (a == null) return;
        
        invalidUuids.add(a.uuid);
        invalidTitles.add(a.title);
        
        uuidToArticle.remove(a.uuid, a);
        titleToArticle.remove(a.title, a);
        
        synchronized (validArticles) {
            validArticles.removeIf(x -> x.uuid.equals(a.uuid));
        }
        
        if (a.categories != null) {
            for (String c : a.categories) {
                String norm = c.replace(",", "").replaceAll("\\s+", "_");
                List<String> uuids = categoryArticles.get(norm);
                if (uuids != null) {
                    uuids.remove(a.uuid);
                }
            }
        }
        
        if (a.language != null) {
            List<String> uuids = languageArticles.get(a.language);
            if (uuids != null) {
                uuids.remove(a.uuid);
            }
        }
        
        Set<String> kws = keywordsPerArticle.remove(a.uuid);
        if (kws != null) {
            for (String w : kws) {
                AtomicInteger cnt = keywordsCount.get(w);
                if (cnt != null && cnt.decrementAndGet() <= 0) {
                    keywordsCount.remove(w, cnt);
                }
            }
        }
        
        if (a.author != null) {
            AtomicInteger cnt = authorCounts.get(a.author);
            if (cnt != null && cnt.decrementAndGet() <= 0) {
                authorCounts.remove(a.author, cnt);
            }
        }
    }
    
    private Set<String> extractKeywords(String text) {
        String[] words = text.split("\\s+");
        Set<String> unique = new HashSet<>();
        
        for (String w : words) {
            if (w.isEmpty()) continue;
            
            String clean = w.toLowerCase().replaceAll("[^a-z]", "");
            if (clean.isEmpty()) continue;
            
            if (linkingWords.contains(clean)) continue;
            
            unique.add(clean);
        }
        return unique;
    }
    
    public void generateOutputs() throws IOException {
        // Generate all_articles.txt
        List<Article> allArticles = new ArrayList<>(validArticles);
        allArticles.sort((a1, a2) -> {
            int cmp = a2.published.compareTo(a1.published);
            if (cmp == 0) {
                return a1.uuid.compareTo(a2.uuid);
            }
            return cmp;
        });
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("all_articles.txt"))) {
            for (Article article : allArticles) {
                writer.write(article.uuid + " " + article.published + "\n");
            }
        }
        
        // Generate category files
        for (String normCategory : categoryArticles.keySet()) {
            List<String> uuids = categoryArticles.get(normCategory);
            if (uuids != null && !uuids.isEmpty()) {
                String filename = normCategory + ".txt";
                List<String> sortedUuids = new ArrayList<>(uuids);
                Collections.sort(sortedUuids);
                
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                    for (String uuid : sortedUuids) {
                        writer.write(uuid + "\n");
                    }
                }
            }
        }
        
        // Generate language files
        for (String language : languageArticles.keySet()) {
            List<String> uuids = languageArticles.get(language);
            if (uuids != null && !uuids.isEmpty()) {
                String filename = language + ".txt";
                List<String> sortedUuids = new ArrayList<>(uuids);
                Collections.sort(sortedUuids);
                
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                    for (String uuid : sortedUuids) {
                        writer.write(uuid + "\n");
                    }
                }
            }
        }
        
        // Generate keywords_count.txt
        List<Map.Entry<String, Integer>> keywordList = new ArrayList<>();
        for (Map.Entry<String, AtomicInteger> entry : keywordsCount.entrySet()) {
            keywordList.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().get()));
        }
        keywordList.sort((e1, e2) -> {
            int cmp = e2.getValue().compareTo(e1.getValue());
            if (cmp == 0) {
                return e1.getKey().compareTo(e2.getKey());
            }
            return cmp;
        });
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("keywords_count.txt"))) {
            for (Map.Entry<String, Integer> entry : keywordList) {
                writer.write(entry.getKey() + " " + entry.getValue() + "\n");
            }
        }
        
        // Generate reports.txt
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("reports.txt"))) {
            writer.write("duplicates_found - " + duplicatesCount.get() + "\n");
            writer.write("unique_articles - " + validArticles.size() + "\n");
            
            // Best author
            String bestAuthor = "-";
            int bestAuthorCount = 0;
            for (Map.Entry<String, AtomicInteger> entry : authorCounts.entrySet()) {
                int count = entry.getValue().get();
                if (count > bestAuthorCount || (count == bestAuthorCount && entry.getKey().compareTo(bestAuthor) < 0)) {
                    bestAuthor = entry.getKey();
                    bestAuthorCount = count;
                }
            }
            writer.write("best_author - " + bestAuthor + " " + bestAuthorCount + "\n");
            
            // Top language - count all articles by language
            ConcurrentHashMap<String, AtomicInteger> languageCounts = new ConcurrentHashMap<>();
            for (Article a : validArticles) {
                if (a.language != null && !a.language.isEmpty()) {
                    languageCounts.computeIfAbsent(a.language, k -> new AtomicInteger()).incrementAndGet();
                }
            }
            String topLanguage = "-";
            int topLanguageCount = 0;
            for (Map.Entry<String, AtomicInteger> entry : languageCounts.entrySet()) {
                int count = entry.getValue().get();
                if (count > topLanguageCount || (count == topLanguageCount && entry.getKey().compareTo(topLanguage) < 0)) {
                    topLanguage = entry.getKey();
                    topLanguageCount = count;
                }
            }
            writer.write("top_language - " + topLanguage + " " + topLanguageCount + "\n");
            
            // Top category - count all articles by category (normalized)
            ConcurrentHashMap<String, AtomicInteger> categoryCounts = new ConcurrentHashMap<>();
            for (Article a : validArticles) {
                if (a.categories != null) {
                    Set<String> uniqueCats = new HashSet<>();
                    for (String c : a.categories) {
                        String norm = c.replace(",", "").replaceAll("\\s+", "_");
                        if (normalizedToReal.containsKey(norm)) {
                            uniqueCats.add(norm);
                        }
                    }
                    for (String norm : uniqueCats) {
                        categoryCounts.computeIfAbsent(norm, k -> new AtomicInteger()).incrementAndGet();
                    }
                }
            }
            String topCategory = "-";
            int topCategoryCount = 0;
            for (Map.Entry<String, AtomicInteger> entry : categoryCounts.entrySet()) {
                int count = entry.getValue().get();
                if (count > topCategoryCount || (count == topCategoryCount && entry.getKey().compareTo(topCategory) < 0)) {
                    topCategory = entry.getKey();
                    topCategoryCount = count;
                }
            }
            writer.write("top_category - " + topCategory + " " + topCategoryCount + "\n");
            
            // Most recent article
            if (!allArticles.isEmpty()) {
                Article mostRecent = allArticles.get(0);
                writer.write("most_recent_article - " + mostRecent.published + " " + mostRecent.url + "\n");
            } else {
                writer.write("most_recent_article - - -\n");
            }
            
            // Top keyword
            if (!keywordList.isEmpty()) {
                Map.Entry<String, Integer> topKeyword = keywordList.get(0);
                writer.write("top_keyword_en - " + topKeyword.getKey() + " " + topKeyword.getValue() + "\n");
            } else {
                writer.write("top_keyword_en - - 0\n");
            }
        }
    }
}