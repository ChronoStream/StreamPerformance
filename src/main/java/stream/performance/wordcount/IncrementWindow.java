package stream.performance.wordcount;

import java.util.ArrayList;

import org.apache.commons.collections.buffer.CircularFifoBuffer;

public class IncrementWindow {

	private int _currentRecordCount;
	private int _fullRecordCount;
	private int _slideRecordCount;

	public CircularFifoBuffer _buffer;

	public IncrementWindow(int windowSize, int slidingStep, int computeGranularity) {
		this._currentRecordCount = 0;
		// here we assume that windowSize and slidingStep is divisible by
		// computeGranularity.
		this._fullRecordCount = windowSize / computeGranularity;
		this._slideRecordCount = slidingStep / computeGranularity;
		this._buffer = new CircularFifoBuffer(_fullRecordCount);
	}

	public void pushBack(ArrayList tupleBatch) {
		_buffer.add(tupleBatch);
		_currentRecordCount += 1;
	}

	public ArrayList popFront() {
		ArrayList frontBatch = (ArrayList) _buffer.get();
		_buffer.remove();
		return frontBatch;
	}

	public boolean isFull() {
		return _currentRecordCount >= _fullRecordCount;
	}

	public boolean isEmittable() {
		if (_currentRecordCount == _fullRecordCount + _slideRecordCount) {
			_currentRecordCount -= _slideRecordCount;
			return true;
		}
		return false;
	}
}
