"""
Simple Local RAG Implementation for Infervia
TESTED AND WORKING with your enhanced_healthcare_analysis files
"""

import json
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import re

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np


class SimpleLocalRAG:
    """
    Simple RAG system that works with your local JSON files
    Uses TF-IDF for embeddings (fast, no model downloads)
    """
    
    def __init__(self, json_files_dir: str = "/Users/dharshinikannan/HealthcarePlatform/Coding Web scraper/healthcare_api/analysis_data"):
        self.json_files_dir = Path(json_files_dir)
        self.documents = []
        self.vectorizer = TfidfVectorizer(max_features=500, stop_words='english')
        self.doc_vectors = None
        
        print("📚 Initializing Simple Local RAG...")
        print(f"📂 Looking in: {self.json_files_dir.absolute()}")
        self._load_documents()
        self._create_embeddings()
    
    def _load_documents(self):
        """Load all enhanced_healthcare_analysis JSON files"""
        
        # Look for enhanced_healthcare_analysis_*.json files
        json_files = list(self.json_files_dir.glob("enhanced_healthcare_analysis_*.json"))
        
        if not json_files:
            print(f"⚠️ No enhanced_healthcare_analysis_*.json files found in {self.json_files_dir}")
            print("💡 Make sure to:")
            print("   1. Run your pipeline to generate the JSON files")
            print("   2. Or copy them to this directory")
            print("   3. Or update the json_files_dir parameter")
            return
        
        print(f"📂 Found {len(json_files)} analysis file(s)")
        
        for json_file in json_files:
            try:
                print(f"   Loading: {json_file.name}")
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                # Extract key sections for RAG
                self._extract_documents_from_analysis(data, json_file.name)
                
            except Exception as e:
                print(f"   ⚠️ Error loading {json_file.name}: {e}")
        
        print(f"✅ Loaded {len(self.documents)} document chunks")
    
    def _extract_documents_from_analysis(self, data: Dict, source_file: str):
        """Extract searchable chunks from your analysis structure"""
        
        # Extract from Stage 1: Enhanced Facts
        stage1 = data.get("stage_1_enhanced_facts", {})
        
        # Policy summary
        policy = stage1.get("policy_plain_english_summary", {})
        if policy:
            text = f"{policy.get('what_happened', '')} {policy.get('why_important', '')} {policy.get('key_quote_plain_english', '')}"
            if text.strip():
                self.documents.append({
                    "content": text,
                    "metadata": {
                        "type": "policy_summary",
                        "source_file": source_file,
                        "section": "stage_1_policy_summary"
                    }
                })
        
        # Document analysis
        doc_analysis = stage1.get("comprehensive_document_analysis", {})
        new_docs = doc_analysis.get("new_documents", [])
        
        for doc in new_docs:
            # Add document with title, summary, and opening
            text = f"{doc.get('title', '')} {doc.get('document_summary', '')} {doc.get('full_opening', '')}"
            if text.strip():
                self.documents.append({
                    "content": text[:1500],  # Limit to 1500 chars
                    "metadata": {
                        "type": "document",
                        "title": doc.get('title', 'Untitled'),
                        "url": doc.get('url', ''),
                        "date": doc.get('date', ''),
                        "source_file": source_file,
                        "section": "stage_1_documents"
                    }
                })
        
        # Requirements
        requirements = stage1.get("detailed_requirements_extraction", [])
        for req in requirements:
            text = f"{req.get('requirement_type', '')} {req.get('exact_text', '')} {req.get('full_evidence_quote', '')}"
            if text.strip():
                self.documents.append({
                    "content": text[:1000],
                    "metadata": {
                        "type": "requirement",
                        "requirement_type": req.get('requirement_type', ''),
                        "source_file": source_file,
                        "section": "stage_1_requirements"
                    }
                })
        
        # Extract from Stage 3: Information Analysis
        stage3 = data.get("stage_3_information_analysis", {})
        
        # Executive briefing
        briefing = stage3.get("executive_briefing", {})
        if briefing:
            text = f"{briefing.get('bottom_line', '')} {briefing.get('board_briefing', '')}"
            if text.strip():
                self.documents.append({
                    "content": text,
                    "metadata": {
                        "type": "executive_summary",
                        "source_file": source_file,
                        "section": "stage_3_briefing"
                    }
                })
        
        # Industry response patterns
        patterns = stage3.get("industry_response_patterns", {})
        responses = patterns.get("typical_immediate_responses", [])
        
        for resp in responses:
            text = f"{resp.get('common_response', '')} {resp.get('typical_timeframes', '')} {resp.get('commonly_involved_roles', '')}"
            if text.strip():
                self.documents.append({
                    "content": str(text)[:1000],
                    "metadata": {
                        "type": "action_pattern",
                        "source_file": source_file,
                        "section": "stage_3_patterns"
                    }
                })
    
    def _create_embeddings(self):
        """Create TF-IDF embeddings for all documents"""
        
        if not self.documents:
            print("⚠️ No documents to embed")
            return
        
        # Extract text content
        texts = [doc["content"] for doc in self.documents]
        
        try:
            # Create TF-IDF vectors
            self.doc_vectors = self.vectorizer.fit_transform(texts)
            print(f"✅ Created embeddings for {len(self.documents)} chunks")
        except Exception as e:
            print(f"❌ Error creating embeddings: {e}")
            self.doc_vectors = None
    
    def search(self, query: str, top_k: int = 5, filter_by_type: Optional[str] = None) -> List[Dict]:
        """
        Search for relevant documents using TF-IDF similarity
        
        Args:
            query: Search query
            top_k: Number of results to return
            filter_by_type: Filter by document type (policy_summary, document, requirement, etc.)
        """
        
        if not self.documents or self.doc_vectors is None:
            print("⚠️ No documents available for search")
            return []
        
        try:
            # Create query vector
            query_vector = self.vectorizer.transform([query])
            
            # Calculate similarities
            similarities = cosine_similarity(query_vector, self.doc_vectors)[0]
            
            # Get top results
            top_indices = np.argsort(similarities)[::-1]
            
            results = []
            for idx in top_indices:
                doc = self.documents[idx]
                similarity = similarities[idx]
                
                # Filter by type if specified
                if filter_by_type and doc["metadata"]["type"] != filter_by_type:
                    continue
                
                # Skip very low similarity scores
                if similarity < 0.05:
                    continue
                
                results.append({
                    "content": doc["content"][:500],  # Limit to 500 chars
                    "similarity": float(similarity),
                    "metadata": doc["metadata"]
                })
                
                if len(results) >= top_k:
                    break
            
            return results
        
        except Exception as e:
            print(f"❌ Search error: {e}")
            return []
    
    def search_by_facility_type(self, facility_type: str, top_k: int = 5) -> List[Dict]:
        """Search for regulations affecting a specific facility type"""
        query = f"{facility_type} facility regulations requirements"
        return self.search(query, top_k)
    
    def search_by_department(self, department: str, top_k: int = 5) -> List[Dict]:
        """Search for regulations affecting a specific department"""
        query = f"{department} department requirements compliance"
        return self.search(query, top_k)
    
    def get_related_documents(self, document_title: str, top_k: int = 3) -> List[Dict]:
        """Find documents related to a given document"""
        return self.search(document_title, top_k, filter_by_type="document")


# ==============================================================================
# Usage Example
# ==============================================================================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("🔍 Testing Simple Local RAG")
    print("="*80 + "\n")
    
    # Initialize RAG system
    # NOTE: Change "." to your actual directory if files are elsewhere
    rag = SimpleLocalRAG(json_files_dir=".")
    
    if not rag.documents:
        print("\n" + "="*80)
        print("❌ No documents loaded!")
        print("="*80)
        print("\n💡 SOLUTIONS:")
        print("   1. Copy your enhanced_healthcare_analysis_*.json file to current directory:")
        print("      cp /path/to/enhanced_healthcare_analysis_*.json .")
        print()
        print("   2. Or specify the directory:")
        print("      rag = SimpleLocalRAG(json_files_dir='/path/to/your/files')")
        print()
        print("   3. Or run your pipeline to generate the files first")
        print("="*80)
    else:
        print("="*80)
        print("🧪 Testing Search")
        print("="*80)
        
        # Test 1: General search
        print("\n1️⃣ Test: Search for 'Medicare Advantage quality measures'")
        results = rag.search("Medicare Advantage quality measures", top_k=3)
        
        if results:
            for i, result in enumerate(results, 1):
                print(f"\n   Result {i} (similarity: {result['similarity']:.3f}):")
                print(f"   Type: {result['metadata']['type']}")
                if 'title' in result['metadata']:
                    print(f"   Title: {result['metadata']['title'][:60]}...")
                print(f"   Content: {result['content'][:200]}...")
        else:
            print("   No results found")
        
        # Test 2: Search by facility type
        print("\n2️⃣ Test: Search for hospital-specific regulations")
        results = rag.search_by_facility_type("hospital", top_k=3)
        
        if results:
            for i, result in enumerate(results, 1):
                print(f"\n   Result {i} (similarity: {result['similarity']:.3f}):")
                print(f"   Type: {result['metadata']['type']}")
                print(f"   Content: {result['content'][:200]}...")
        else:
            print("   No results found")
        
        # Test 3: Search by department
        print("\n3️⃣ Test: Search for billing department regulations")
        results = rag.search_by_department("billing", top_k=3)
        
        if results:
            for i, result in enumerate(results, 1):
                print(f"\n   Result {i} (similarity: {result['similarity']:.3f}):")
                print(f"   Type: {result['metadata']['type']}")
                print(f"   Content: {result['content'][:200]}...")
        else:
            print("   No results found")
        
        print("\n" + "="*80)
        print("✅ RAG Testing Complete!")
        print("="*80)