"use client";

import { useState } from "react";
import { motion } from "framer-motion";
import { Upload, FileText, Sparkles, Download, BarChart3, Shield, Zap } from "lucide-react";
import FileUpload from "@/components/FileUpload";
import ProcessingDashboard from "@/components/ProcessingDashboard";
import FeatureCard from "@/components/FeatureCard";
import AnimatedBackground from "@/components/AnimatedBackground";

export default function Home() {
  const [file, setFile] = useState<File | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);

  const features = [
    {
      icon: Sparkles,
      title: "AI-Powered Cleaning",
      description: "Advanced algorithms standardize medical terminology",
      color: "from-purple-500 to-pink-500"
    },
    {
      icon: Shield,
      title: "HIPAA Compliant",
      description: "Secure processing with privacy protection",
      color: "from-green-500 to-emerald-500"
    },
    {
      icon: Zap,
      title: "Lightning Fast",
      description: "Process thousands of records in seconds",
      color: "from-blue-500 to-cyan-500"
    },
    {
      icon: BarChart3,
      title: "Smart Analytics",
      description: "Detailed insights and quality metrics",
      color: "from-orange-500 to-red-500"
    }
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-100 relative overflow-hidden">
      <AnimatedBackground />
      
      {/* Header */}
      <header className="relative z-10 p-6">
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          className="flex items-center gap-3"
        >
          <div className="w-12 h-12 bg-gradient-to-r from-blue-600 to-purple-600 rounded-xl flex items-center justify-center">
            <FileText className="w-6 h-6 text-white" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Healthcare Data Cleaner</h1>
            <p className="text-gray-600 text-sm">AI-Powered Medical Data Processing</p>
          </div>
        </motion.div>
      </header>

      {/* Main Content */}
      <main className="relative z-10 max-w-7xl mx-auto px-6 py-8">
        {!file ? (
          <>
            {/* Hero Section */}
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 }}
              className="text-center mb-16"
            >
              <h2 className="text-5xl md:text-7xl font-bold text-gray-900 mb-6">
                Transform Your{" "}
                <span className="bg-gradient-to-r from-blue-600 via-purple-600 to-pink-600 bg-clip-text text-transparent">
                  Healthcare Data
                </span>
              </h2>
              <p className="text-xl text-gray-600 max-w-3xl mx-auto mb-8">
                Standardize medical terminology, clean inconsistent data, and ensure HIPAA compliance 
                with our advanced AI-powered processing engine.
              </p>
              
              <motion.div
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: 0.3 }}
              >
                <FileUpload onFileSelect={setFile} />
              </motion.div>
            </motion.div>

            {/* Features Grid */}
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.5 }}
              className="grid md:grid-cols-2 lg:grid-cols-4 gap-6 mb-16"
            >
              {features.map((feature, index) => (
                <motion.div
                  key={feature.title}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.6 + index * 0.1 }}
                >
                  <FeatureCard {...feature} />
                </motion.div>
              ))}
            </motion.div>

            {/* Stats Section */}
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.8 }}
              className="bg-white/60 backdrop-blur-sm border border-white/20 rounded-3xl p-8 shadow-2xl"
            >
              <div className="grid md:grid-cols-3 gap-8 text-center">
                <div>
                  <div className="text-4xl font-bold text-blue-600 mb-2">99.9%</div>
                  <div className="text-gray-600">Accuracy Rate</div>
                </div>
                <div>
                  <div className="text-4xl font-bold text-green-600 mb-2">10K+</div>
                  <div className="text-gray-600">Records Processed</div>
                </div>
                <div>
                  <div className="text-4xl font-bold text-purple-600 mb-2">&lt; 1s</div>
                  <div className="text-gray-600">Average Processing Time</div>
                </div>
              </div>
            </motion.div>
          </>
        ) : (
          <ProcessingDashboard 
            file={file} 
            onBack={() => setFile(null)}
            isProcessing={isProcessing}
            onProcessingChange={setIsProcessing}
          />
        )}
      </main>
    </div>
  );
}
