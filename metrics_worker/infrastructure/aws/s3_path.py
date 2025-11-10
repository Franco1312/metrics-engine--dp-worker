"""S3 path utilities."""


class S3Path:
    """S3 path utilities for normalizing and handling S3 paths."""

    S3_PREFIX = "s3://"
    S3_PREFIX_LENGTH = len(S3_PREFIX)

    @staticmethod
    def normalize(path: str) -> str:
        """Normalize S3 path by removing s3:// prefix if present.
        
        Args:
            path: S3 path (with or without s3:// prefix)
            
        Returns:
            Normalized path without s3:// prefix
        """
        if path.startswith(S3Path.S3_PREFIX):
            return path[S3Path.S3_PREFIX_LENGTH:]
        return path

    @staticmethod
    def to_full_path(bucket: str, key: str) -> str:
        """Build full S3 path from bucket and key.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            Full S3 path (s3://bucket/key)
        """
        return f"{S3Path.S3_PREFIX}{bucket}/{key.lstrip('/')}"

    @staticmethod
    def join(*parts: str) -> str:
        """Join path parts, normalizing separators.
        
        Args:
            *parts: Path parts to join
            
        Returns:
            Joined path with normalized separators
        """
        normalized_parts = [part.strip("/") for part in parts if part]
        return "/".join(normalized_parts)

    @staticmethod
    def parent(path: str) -> str:
        """Get parent directory of a path.
        
        Args:
            path: Path to get parent from
            
        Returns:
            Parent directory path, or empty string if no parent
        """
        normalized = path.rstrip("/")
        if "/" not in normalized:
            return ""
        return normalized.rsplit("/", 1)[0]

    @staticmethod
    def basename(path: str) -> str:
        """Get the base name (filename) of a path.
        
        Args:
            path: Path to get basename from
            
        Returns:
            Base name (filename) of the path
        """
        normalized = path.rstrip("/")
        if "/" not in normalized:
            return normalized
        return normalized.rsplit("/", 1)[-1]

    @staticmethod
    def stem(path: str) -> str:
        """Get the stem (filename without extension) of a path.
        
        Args:
            path: Path to get stem from
            
        Returns:
            Stem (filename without extension) of the path
        """
        basename = S3Path.basename(path)
        if "." not in basename:
            return basename
        return basename.rsplit(".", 1)[0]

    @staticmethod
    def suffix(path: str) -> str:
        """Get the suffix (extension) of a path.
        
        Args:
            path: Path to get suffix from
            
        Returns:
            Suffix (extension) of the path, including the dot, or empty string
        """
        basename = S3Path.basename(path)
        if "." not in basename:
            return ""
        return "." + basename.rsplit(".", 1)[-1]

    @staticmethod
    def with_name(path: str, new_name: str) -> str:
        """Replace the filename in a path with a new name.
        
        Args:
            path: Original path
            new_name: New filename to use
            
        Returns:
            Path with replaced filename
        """
        parent = S3Path.parent(path)
        if not parent:
            return new_name
        return S3Path.join(parent, new_name)

    @staticmethod
    def with_suffix(path: str, suffix: str) -> str:
        """Replace the suffix (extension) in a path.
        
        Args:
            path: Original path
            suffix: New suffix (with or without leading dot)
            
        Returns:
            Path with replaced suffix
        """
        if not suffix.startswith("."):
            suffix = "." + suffix
        
        stem = S3Path.stem(path)
        parent = S3Path.parent(path)
        
        if not parent:
            return stem + suffix
        return S3Path.join(parent, stem + suffix)

    @staticmethod
    def rstrip_separator(path: str) -> str:
        """Remove trailing separator from path.
        
        Args:
            path: Path to strip
            
        Returns:
            Path without trailing separator
        """
        return path.rstrip("/")

