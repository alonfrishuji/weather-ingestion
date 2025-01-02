import pytest
from unittest.mock import AsyncMock, patch, Mock

import server.ingestion_service as ingestion_service
from server.models import BatchMetadata, WeatherData
from tests.utils import create_mock_response, create_mock_http_error

  
class TestFetchOperations:
    """Tests for all fetch-related operations."""
    
    @pytest.mark.asyncio
    async def test_fetch_batches_success(self, mock_batches):
        """Test successful batch fetching."""
        mock_response = create_mock_response(json_data=mock_batches)
        
        with patch("httpx.AsyncClient.get", AsyncMock(return_value=mock_response)):
            result = await ingestion_service.fetch_batches()
            assert result == mock_batches
            
    @pytest.mark.asyncio
    async def test_fetch_batches_error(self):
        """Test batch fetching with network error."""
        with patch("httpx.AsyncClient.get", AsyncMock(side_effect=create_mock_http_error())):
            result = await ingestion_service.fetch_batches()
            assert result == []
            
    @pytest.mark.asyncio
    async def test_fetch_batch_data_success(self, mock_batch_data):
        """Test successful batch data fetching."""
        mock_response = create_mock_response(json_data={"data": mock_batch_data})
        
        with patch("httpx.AsyncClient.get", AsyncMock(return_value=mock_response)):
            result = await ingestion_service.fetch_batch_data("batch1", total_pages=1)
            assert result == mock_batch_data
            
    @pytest.mark.asyncio
    async def test_fetch_batch_data_error(self):
        """Test batch data fetching with network error."""
        with patch("httpx.AsyncClient.get", AsyncMock(side_effect=create_mock_http_error())):
            result = await ingestion_service.fetch_batch_data("batch1")
            assert result == []
            
    @pytest.mark.asyncio
    async def test_fetch_total_pages_success(self):
        """Test successful fetching of total pages."""
        mock_response = create_mock_response(json_data={"metadata": {"total_pages": 5}})
        
        with patch("httpx.AsyncClient.get", AsyncMock(return_value=mock_response)):
            result = await ingestion_service.fetch_total_pages("batch1")
            assert result == 5

class TestDatabaseOperations:
    """Tests for all database-related operations."""
    
    def test_delete_old_active_batches(self, mock_db_session, sample_batch_metadata):
        """Test deletion of old active batches."""
        mock_db_session.all.return_value = sample_batch_metadata
        
        with patch("server.ingestion_service.SessionLocal", return_value=mock_db_session):
            ingestion_service.delete_old_active_batches()
            
            assert mock_db_session.commit.called
            assert sample_batch_metadata[0].status == "INACTIVE"
            assert sample_batch_metadata[1].status == "INACTIVE"
            
    def test_delete_weather_data_for_non_retained_batches(self, mock_db_session, sample_batch_metadata):
        """Test deletion of weather data for non-retained batches."""
        non_retained_batches = [b for b in sample_batch_metadata if not b.retained]
        mock_db_session.all.return_value = non_retained_batches
        
        with patch("server.ingestion_service.SessionLocal", return_value=mock_db_session):
            ingestion_service.delete_weather_data_for_non_retained_batches()
            assert mock_db_session.commit.called
            
    def test_batch_insert_weather_data(self, mock_db_session, sample_weather_records):
        """Test batch insertion of weather data."""
        with patch("server.ingestion_service.SessionLocal", return_value=mock_db_session):
            ingestion_service.batch_insert_weather_data(sample_weather_records)
            assert mock_db_session.bulk_save_objects.called
            assert mock_db_session.commit.called

class TestBatchProcessing:
    """Tests for batch processing and ingestion."""
    
    @pytest.mark.asyncio
    async def test_ingest_batch_success(self, mock_db_session, mock_batches, mock_batch_data):
        """Test successful batch ingestion."""
        mock_db_session.query().filter_by().first.return_value = None
        
        with patch("server.ingestion_service.SessionLocal", return_value=mock_db_session), \
             patch("server.ingestion_service.fetch_total_pages", AsyncMock(return_value=1)), \
             patch("server.ingestion_service.fetch_batch_data", AsyncMock(return_value=mock_batch_data)), \
             patch("server.ingestion_service.batch_insert_weather_data"):
            
            await ingestion_service.ingest_batch(mock_batches[0])
            assert mock_db_session.add.called
            assert mock_db_session.commit.called
            
    @pytest.mark.asyncio
    async def test_ingest_batch_duplicate(self, mock_db_session, mock_batches):
        """Test handling of duplicate batch ingestion."""
        existing_batch = BatchMetadata(batch_id="batch1", status="ACTIVE")
        mock_db_session.query().filter_by().first.return_value = existing_batch
        
        with patch("server.ingestion_service.SessionLocal", return_value=mock_db_session):
            await ingestion_service.ingest_batch(mock_batches[0])
            assert not mock_db_session.add.called
            
    @pytest.mark.asyncio
    async def test_process_batches(self, mock_batches):
        """Test the complete batch processing workflow."""
        with patch("server.ingestion_service.fetch_batches", AsyncMock(return_value=mock_batches)), \
             patch("server.ingestion_service.ingest_batch", AsyncMock()) as mock_ingest, \
             patch("server.ingestion_service.delete_old_active_batches") as mock_delete_old, \
             patch("server.ingestion_service.delete_weather_data_for_non_retained_batches") as mock_delete_non_retained, \
             patch("server.ingestion_service.retain_metadata_for_deleted_batches") as mock_retain:
            
            await ingestion_service.process_batches()
            
            assert mock_ingest.call_count == len(mock_batches)
            assert mock_delete_old.called
            assert mock_delete_non_retained.called
            assert mock_retain.called

