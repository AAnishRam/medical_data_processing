"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { ArrowLeft, Play, Download, CheckCircle, AlertCircle, BarChart3 } from "lucide-react";

interface ProcessingDashboardProps {
  file: File;
  onBack: () => void;
  isProcessing: boolean;
  onProcessingChange: (processing: boolean) => void;
}

interface ProcessingStats {
  totalRows: number;
  processedRows: number;
  errorRows: number;
  confidenceScore: number;
  processingTime: number;
}

export default function ProcessingDashboard({ 
  file, 
  onBack, 
  isProcessing, 
  onProcessingChange 
}: ProcessingDashboardProps) {
  const [stats, setStats] = useState<ProcessingStats>({
    totalRows: 0,
    processedRows: 0,
    errorRows: 0,
    confidenceScore: 0,
    processingTime: 0,
  });
  
  const [progress, setProgress] = useState(0);
  const [currentStep, setCurrentStep] = useState("Analyzing file structure...");

  // Simulate processing steps
  useEffect(() => {
    if (isProcessing) {
      const steps = [
        "Analyzing file structure...",
        "Validating data integrity...",
        "Standardizing medical terminology...",
        "Applying AI cleaning algorithms...",
        "Generating quality reports...",
        "Finalizing cleaned dataset...",
      ];

      let currentStepIndex = 0;
      const interval = setInterval(() => {
        setProgress((prev) => {
          const newProgress = prev + Math.random() * 15;
          if (newProgress >= 100) {
            clearInterval(interval);
            onProcessingChange(false);
            setStats({
              totalRows: 847,
              processedRows: 842,
              errorRows: 5,
              confidenceScore: 94.7,
              processingTime: 3.2,
            });
            return 100;
          }
          
          // Update step
          const stepProgress = Math.floor(newProgress / (100 / steps.length));
          if (stepProgress < steps.length && stepProgress !== currentStepIndex) {
            currentStepIndex = stepProgress;
            setCurrentStep(steps[stepProgress]);
          }
          
          return newProgress;
        });
      }, 200);

      return () => clearInterval(interval);
    }
  }, [isProcessing, onProcessingChange]);

  const startProcessing = () => {
    onProcessingChange(true);
    setProgress(0);
    setCurrentStep("Initializing...");
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className="max-w-6xl mx-auto"
    >
      {/* Header */}
      <div className="flex items-center gap-4 mb-8">
        <motion.button
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.95 }}
          onClick={onBack}
          className="p-3 bg-white/60 backdrop-blur-sm border border-white/20 rounded-xl hover:bg-white/80 transition-all"
        >
          <ArrowLeft className="w-5 h-5" />
        </motion.button>
        
        <div>
          <h2 className="text-3xl font-bold text-gray-900">Processing Dashboard</h2>
          <p className="text-gray-600">File: {file.name}</p>
        </div>
      </div>

      {/* File Info Card */}
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ delay: 0.1 }}
        className="healthcare-card p-6 mb-8"
      >
        <div className="grid md:grid-cols-3 gap-6">
          <div>
            <div className="text-sm text-gray-500 mb-1">File Size</div>
            <div className="text-2xl font-bold text-gray-900">
              {(file.size / 1024 / 1024).toFixed(2)} MB
            </div>
          </div>
          <div>
            <div className="text-sm text-gray-500 mb-1">File Type</div>
            <div className="text-2xl font-bold text-gray-900">
              {file.name.split('.').pop()?.toUpperCase()}
            </div>
          </div>
          <div>
            <div className="text-sm text-gray-500 mb-1">Status</div>
            <div className="flex items-center gap-2">
              {isProcessing ? (
                <>
                  <div className="w-3 h-3 bg-blue-500 rounded-full animate-pulse"></div>
                  <span className="text-blue-600 font-semibold">Processing</span>
                </>
              ) : progress === 100 ? (
                <>
                  <CheckCircle className="w-5 h-5 text-green-500" />
                  <span className="text-green-600 font-semibold">Complete</span>
                </>
              ) : (
                <>
                  <div className="w-3 h-3 bg-gray-400 rounded-full"></div>
                  <span className="text-gray-600 font-semibold">Ready</span>
                </>
              )}
            </div>
          </div>
        </div>
      </motion.div>

      {/* Processing Controls */}
      {!isProcessing && progress < 100 && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
          className="text-center mb-8"
        >
          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={startProcessing}
            className="px-8 py-4 bg-gradient-to-r from-blue-600 to-purple-600 text-white rounded-xl font-semibold text-lg hover:shadow-lg transition-all flex items-center gap-3 mx-auto"
          >
            <Play className="w-5 h-5" />
            Start AI Processing
          </motion.button>
        </motion.div>
      )}

      {/* Progress Section */}
      {(isProcessing || progress > 0) && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
          className="healthcare-card p-8 mb-8"
        >
          <div className="mb-6">
            <div className="flex justify-between items-center mb-2">
              <h3 className="text-xl font-bold text-gray-900">Processing Progress</h3>
              <span className="text-2xl font-bold text-blue-600">{Math.round(progress)}%</span>
            </div>
            
            <div className="w-full bg-gray-200 rounded-full h-3 mb-4">
              <motion.div
                className="bg-gradient-to-r from-blue-500 to-purple-500 h-3 rounded-full"
                initial={{ width: 0 }}
                animate={{ width: `${progress}%` }}
                transition={{ duration: 0.5 }}
              />
            </div>
            
            <p className="text-gray-600">{currentStep}</p>
          </div>
        </motion.div>
      )}

      {/* Results Section */}
      {progress === 100 && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
          className="space-y-8"
        >
          {/* Stats Grid */}
          <div className="grid md:grid-cols-4 gap-6">
            <div className="healthcare-card p-6 text-center">
              <div className="text-3xl font-bold text-blue-600 mb-2">{stats.totalRows}</div>
              <div className="text-gray-600">Total Rows</div>
            </div>
            <div className="healthcare-card p-6 text-center">
              <div className="text-3xl font-bold text-green-600 mb-2">{stats.processedRows}</div>
              <div className="text-gray-600">Processed</div>
            </div>
            <div className="healthcare-card p-6 text-center">
              <div className="text-3xl font-bold text-purple-600 mb-2">{stats.confidenceScore}%</div>
              <div className="text-gray-600">Confidence</div>
            </div>
            <div className="healthcare-card p-6 text-center">
              <div className="text-3xl font-bold text-orange-600 mb-2">{stats.processingTime}s</div>
              <div className="text-gray-600">Processing Time</div>
            </div>
          </div>

          {/* Download Section */}
          <div className="healthcare-card p-8 text-center">
            <CheckCircle className="w-16 h-16 text-green-500 mx-auto mb-4" />
            <h3 className="text-2xl font-bold text-gray-900 mb-2">Processing Complete!</h3>
            <p className="text-gray-600 mb-6">
              Your healthcare data has been successfully cleaned and standardized.
            </p>
            
            <div className="flex justify-center gap-4">
              <motion.button
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                className="px-6 py-3 bg-gradient-to-r from-green-600 to-emerald-600 text-white rounded-xl font-semibold hover:shadow-lg transition-all flex items-center gap-2"
              >
                <Download className="w-5 h-5" />
                Download Cleaned Data
              </motion.button>
              
              <motion.button
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                className="px-6 py-3 bg-white border border-gray-300 text-gray-700 rounded-xl font-semibold hover:shadow-lg transition-all flex items-center gap-2"
              >
                <BarChart3 className="w-5 h-5" />
                View Detailed Report
              </motion.button>
            </div>
          </div>
        </motion.div>
      )}
    </motion.div>
  );
}
