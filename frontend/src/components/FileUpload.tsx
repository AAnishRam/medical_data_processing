"use client";

import { useCallback } from "react";
import { motion } from "framer-motion";
import { Upload, FileSpreadsheet, Database } from "lucide-react";

interface FileUploadProps {
  onFileSelect: (file: File) => void;
}

export default function FileUpload({ onFileSelect }: FileUploadProps) {
  const handleDrop = useCallback(
    (e: React.DragEvent<HTMLDivElement>) => {
      e.preventDefault();
      const files = e.dataTransfer.files;
      if (files.length > 0) {
        onFileSelect(files[0]);
      }
    },
    [onFileSelect]
  );

  const handleFileInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const files = e.target.files;
      if (files && files.length > 0) {
        onFileSelect(files[0]);
      }
    },
    [onFileSelect]
  );

  return (
    <motion.div
      whileHover={{ scale: 1.02 }}
      whileTap={{ scale: 0.98 }}
      className="max-w-2xl mx-auto"
    >
      <div
        className="border-2 border-dashed border-blue-300 rounded-3xl p-12 text-center bg-white/40 backdrop-blur-sm hover:bg-white/60 transition-all duration-300 cursor-pointer group"
        onDrop={handleDrop}
        onDragOver={(e) => e.preventDefault()}
        onClick={() => document.getElementById("file-input")?.click()}
      >
        <motion.div
          className="mb-6"
          animate={{ 
            rotateY: [0, 10, -10, 0],
            scale: [1, 1.05, 1] 
          }}
          transition={{ 
            duration: 4,
            repeat: Infinity,
            repeatType: "reverse"
          }}
        >
          <div className="w-20 h-20 bg-gradient-to-r from-blue-500 to-purple-500 rounded-2xl mx-auto flex items-center justify-center group-hover:shadow-lg transition-shadow">
            <Upload className="w-10 h-10 text-white" />
          </div>
        </motion.div>

        <h3 className="text-2xl font-bold text-gray-900 mb-3">
          Upload Your Healthcare Data
        </h3>
        <p className="text-gray-600 mb-6">
          Drop your Excel (.xlsx) or CSV file here, or click to browse
        </p>

        <div className="flex justify-center gap-4 mb-6">
          <div className="flex items-center gap-2 text-sm text-gray-500">
            <FileSpreadsheet className="w-4 h-4" />
            Excel (.xlsx)
          </div>
          <div className="flex items-center gap-2 text-sm text-gray-500">
            <Database className="w-4 h-4" />
            CSV (.csv)
          </div>
        </div>

        <motion.button
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.95 }}
          className="px-8 py-3 bg-gradient-to-r from-blue-600 to-purple-600 text-white rounded-xl font-semibold hover:shadow-lg transition-all"
        >
          Choose File
        </motion.button>

        <input
          id="file-input"
          type="file"
          accept=".xlsx,.csv"
          onChange={handleFileInput}
          className="hidden"
        />
      </div>
    </motion.div>
  );
}
